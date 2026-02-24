from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import json
from functools import reduce

#-----------------------------------------------
# 1. PARAMETERS DECLARATION
#-----------------------------------------------
# Initial setup
# Use the following to enable schema evolution but it could go dangerous if unmanaged - better to raise error and fix schema as in #7
# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true") - # Used in MERGE function (Section 5)
spark.conf.set("spark.sql.shuffle.partitions", "20")

# Capturing metadata (ETag and file paths) for the incoming files as one string string and converting it to a list
dbutils.widgets.text("file_metadata", "")
file_metadata = json.loads(dbutils.widgets.get("file_metadata"))

# If the file_metadata is not a list array but a dictionary
if isinstance(file_metadata, dict):
    file_metadata = [file_metadata]

if not file_metadata:
    raise ValueError("No file paths provided to Silver job")

# Extracting the file paths seperately
file_paths = [f["file_path"] for f in file_metadata]

# Creating ETag mapping DataFrame
etag_mapping_df = spark.createDataFrame(file_metadata)
    
#-----------------------------------------------
# 2. VALIDATION ENGINE
#-----------------------------------------------
rejected_dfs = []
def process_rejections (rejected_df, reject_reason, raw_json_expr):
        
    cols = rejected_df.columns
    
    if "event" in cols:
        # ENVELOPE LEVEL: event is a struct, use its subfields
        e_id = col("event.event_id")
        e_type = col("event.event_type")
    elif "event_id" in cols:
        # DOMAIN LEVEL: event_id is already flattened
        e_id = col("event_id")
        e_type = col("event_type")
    else:
        # CONTAINER LEVEL: No events exist yet, use None
        e_id = lit(None)
        e_type = lit(None)
        
    df_to_write = rejected_df.select(
        col("file_id"),
        col("domain"),
        col("source_system"),
        col("created_at"),
        col("etag"),
        e_id.alias("event_id"),
        e_type.alias("event_type"),
        reject_reason.alias("rejection_reason"),
        raw_json_expr.alias("raw_event_json"),
        current_timestamp().alias("rejection_ts")
        ).withColumn("rejection_date", to_date(col("rejection_ts")))
 
    rejected_dfs.append(df_to_write)
#-----------------------------------------------
# 3. FILE-LEVEL VALIDATION AND PROCESSING
#-----------------------------------------------
# Reading the JSON files from the list of paths along with the 'hidden' file_path to map the Etag downstream 
raw_df = spark.read.option("multiLine", True).json(file_paths)\
            .withColumn("file_path", input_file_name())
            
raw_df = raw_df.join(etag_mapping_df, "file_path", left)

# Basic file-level validation: Check for missing columns
required_cols = ["file_id", "domain", "source_system", "created_at", "events"]
missing_cols = [c for c in required_cols if c not in raw_df.columns]
if missing_cols:
    raise ValueError(f"Missing required columns:{missing_cols}")

# Basic file-level validation: Check for nulls and extracting them to rejected table
bad_cont_cond = (
    col("file_id").isNull() |
    col("domain").isNull() |
    col("source_system").isNull() |
    col("created_at").isNull() |
    col("events").isNull() | (size(col("events"))) == 0
)

process_rejections(
    raw_df.filter(bad_cont_cond), 
    concat_ws(
        ",",
        when(col("file_id").isNull(), lit("MISSING_FILE_ID")),
        when(col("domain").isNull(), lit("MISSING_DOMAIN")),
        when(col("source_system").isNull(), lit("MISSING_SOURCE_SYSTEM")),
        when(col("created_at").isNull(), lit("MISSING_CREATION_TIME")),
        when(col("events").isNull() | (size(col("events")) == 0), lit("EMPTY_EVENTS"))
    ),
    to_json(struct("*"))
)

# Flatten approved container
df_cont = raw_df.filter(
    col("file_id").isNotNull() &
    col("domain").isNotNull() &
    col("source_system").isNotNull() &
    col("created_at").isNotNull()
).select(
    col("file_id"),
    col("etag"),
    col("domain"),
    col("source_system"),
    col("created_at"),
    explode("events").alias("event")
)

df_cont = df_cont.persist()

#-----------------------------------------------
# 4. ENVELOPE-LEVEL VALIDATION AND PROCESSING
#-----------------------------------------------
# Basic envelope-level validation: Check for nulls and extracting them to rejected table
bad_env_cond= (
    col("event.event_id").isNull() |
    col("event.event_type").isNull() |
    col("event.event_ts").isNull() |
    col("event.source").isNull() |
    col("event.ingest_ts").isNull() |
    col("event.ingest_ts") < col ("event.event_ts") |
    col("event.event_ts") > current_timestamp()
)

process_rejections(
    df_cont.filter(bad_env_cond),
    concat_ws(
        ",",
        when(col("event.event_id").isNull(), lit("MISSING_EVENT_ID")),
        when(col("event.event_type").isNull(), lit("MISSING_EVENT_TYPE")),
        when(col("event.event_ts").isNull(), lit("MISSING_EVENT_TIMESTAMP")),
        when(col("event.source").isNull(), lit("MISSING_SOURCE")),
        when(col("event.ingest_ts").isNull(), lit("MISSING_INGEST_TIMESTAMP")),
        when(col("event.ingest_ts") < col ("event.event_ts"), lit("INGEST BEFORE EVENT_TIMESTAMP")),
        when(col("event.event_ts") > current_timestamp(), lit("EVENT_TIMESTAMP_IN_FUTURE")),
    ),
    to_json(col("event"))
)

# Flatten approved envelopes
df_env = df_cont.filter(
    col("event.event_id").isNotNull() &
    col("event.event_type").isNotNull() &
    col("event.event_ts").isNotNull() &
    col("event.source").isNotNull() &
    col("event.ingest_ts").isNotNull() &
    col("event.ingest_ts") >= col ("event.event_ts") &
    col("event.event_ts") <= current_timestamp()
).select(
    col("file_id"),
    col("etag"),
    col("domain"),
    col("source_system"),
    col("created_at"),
    col("event.event_id").alias("event_id"),
    col("event.event_type").alias("event_type"),
    col("event.source").alias("source"),
    col("event.event_ts").alias("event_ts"),
    col("event.ingest_ts").alias("ingest_ts"),
    col("event.payload").alias("payload")
).withColumn("event_date", to_date(col("event_ts")))

df_env = df_env.persist()

# Handling domain mismatch - Extra safety but for this project event_type is indeed different than the domain name as event_type demonstrates the status of it's respective type
#domain_mismatch_cond = col("domain") != col("event_type")

#process_rejections(
#    df_env.filter(domain_mismatch_cond),
#    lit("DOMAIN_EVENT_TYPE_MISMATCH"),
#    to_json(struct("event_id", "event_type", "source", "event_ts","ingest_ts","payload"))
#)
                    
#df_env = df_env.filter(col("domain") == col("event_type"))

# Handling invalid event_type
valid_types = ["user_events", "payment_events", "order_events"]

invalid_type_cond = ~col("event_type").isin(valid_types)

process_rejections(
    df_env.filter(invalid_type_cond),
    lit("INVALID_EVENT_TYPE"),
    to_json(struct("event_id", "event_type", "source", "event_ts","ingest_ts","payload"))
)

df_env = df_env.filter(col("event_type").isin(valid_types))

# Splitting up by event type
user_df = df_env.filter(col("event_type") == "user_events")
payment_df = df_env.filter(col("event_type") == "payment_events")
order_df = df_env.filter(col("event_type") == "order_events")

#-----------------------------------------------
# 5. DOMAIN-LEVEL VALIDATION AND PROCESSING
#-----------------------------------------------
# Handling schema evolution
contract_fields = {
        "user_events": {"user_id", "email", "country", "device"},
        "payment_events": {"payment_id", "order_id", "amount", "currency", "failure_reason"},
        "order_events": {"user_id", "order_id", "amount", "currency", "status"}
}

# Payload structure definition for downstream if the payload is null or string or invalid
valid_user_payload_struct = StructType([
    StructField("user_id", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("device", StringType(), True),
])

valid_payment_payload_struct = StructType([
    StructField("payment_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("amount", DecimalType(10,2), True),
    StructField("currency", StringType(), True),
    StructField("failure_reason", StringType(), True)
])

valid_order_payload_struct = StructType([
    StructField("user_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("amount", DecimalType(10,2), True),
    StructField("currency", StringType(), True),
    StructField("status", StringType(), True)
])

def validate_contract(df, event_type, expected_contract_fields, payload_struct):
    payload_field = [f for f in df.schema.fields if f.name == "payload"][0]
    
    if isinstance(payload_field.dataType, NullType):
        process_rejections(
            df,
            lit("PAYLOAD_IS_NULL"),
            col("payload")
        )
        safe_fields = [f for f in df.schema.fields if f.name != "payload"]
        safe_fields.append(StructField("payload", payload_struct, True))
        return df.sparkSession.createDataFrame([], StructType(safe_fields))
        
    elif isinstance(payload_field.dataType, StringType):
        process_rejections(
            df,
            lit("PAYLOAD_IS_A_STRING"),
            col("payload")
        )
        safe_fields = [f for f in df.schema.fields if f.name != "payload"]
        safe_fields.append(StructField("payload", payload_struct, True))
        return df.sparkSession.createDataFrame([], StructType(safe_fields))
        
    elif isinstance(payload_field.dataType, StructType):
        actual_fields = {f.name for f in payload_field.dataType.fields}
        unexpected_fields = actual_fields - expected_contract_fields
        if unexpected_fields:
            raise Exception(f"Violation for {event_type}! Unexpected field: {unexpected_fields}") 
        return df
        
    else:
        process_rejections(
            df,
            lit("PAYLOAD_IS_INVALID"),
            col("payload")
        )
        safe_fields = [f for f in df.schema.fields if f.name != "payload"]
        safe_fields.append(StructField("payload", payload_struct, True))
        return df.sparkSession.createDataFrame([], StructType(safe_fields))

users_df = validate_contract(user_df, "user_events", contract_fields["user_events"], valid_user_payload_struct)
payments_df = validate_contract(payment_df, "payment_events", contract_fields["payment_events"], valid_payment_payload_struct)
orders_df = validate_contract(order_df, "order_events", contract_fields["order_events"], valid_order_payload_struct)

# Captruing null values and extracting them to rejected tables
users_rejected_cond = col("payload.user_id").isNull()

process_rejections(
    users_df.filter(user_rejected_cond),
    lit("MISSING_USER_ID").alias("rejection_reason"),
    to_json(col("payload")).alias("raw_event_json")
)

users_df = users_df.filter(col("payload.user_id").isNotNull())

payments_rejected_cond = (
    col("payload.payment_id").isNull() |
    col("payload.order_id").isNull() |
    col("payload.amount") < 0
)

process_rejections(
    payments_df.filter(payments_rejected_cond),
    concat_ws(
        ",",
        when(col("payload.payment_id").isNull(), lit("MISSING_PAYMENT_ID")),
        when(col("payload.order_id").isNull(), lit("MISSING_ORDER_ID")),
        when(col("payload.amount") < 0, lit("INVALID AMOUNT"))
    ),
    to_json(col("payload")).alias("raw_event_json"),
)

payments_df = payments_df.filter(
                col("payload.payment_id").isNotNull() & 
                col("payload.order_id").isNotNull() & 
                col("payload.amount") >= 0
              )

orders_rejected_cond = (
    col("payload.order_id").isNull() |
    col("payload.user_id").isNull() |
    col("payload.amount") < 0
)

process_rejections(
    orders_df.filter(orders_rejected_cond),
    concat_ws(
        ",",
        when(col("payload.user_id").isNull(), lit("MISSING_USER_ID")),
        when(col("payload.order_id").isNull(), lit("MISSING_ORDER_ID")),
        when(col("payload.amount") < 0, lit("INVALID AMOUNT"))
    ),
    to_json(col("payload")).alias("raw_event_json")
)

orders_df = orders_df.filter(
                col("payload.order_id").isNotNull() & 
                col("payload.user_id").isNotNull() & 
                col("payload.amount") >= 0
              )

# Payload extraction per type
def common_fields ():
    return [
    col("file_id"),
    col("etag"),
    col("domain"),
    col("source_system"),
    col("created_at"),
    col("event_id"),
    col("event_type"),
    col("source"),
    col("event_ts"),
    col("ingest_ts")
    ]
    
users_payload_df = users_df.select(
    *common_fields(),
    col("payload.user_id").alias("user_id"),
    col("payload.email").alias("email"),
    col("payload.country").alias("country"),
    col("payload.device").alias("device")
)

payments_payload_df = payments_df.select(
    *common_fields(),
    col("payload.payment_id").alias("payment_id"),
    col("payload.order_id").alias("order_id"),
    col("payload.amount").cast("decimal(10,2)").alias("amount"),
    col("payload.currency").alias("currency"),
    col("payload.failure_reason").alias("failure_reason")
)

orders_payload_df = orders_df.select(
    *common_fields(),
    col("payload.order_id").alias("order_id"),
    col("payload.user_id").alias("user_id"),
    col("payload.amount").cast("decimal(10,2)").alias("amount"),
    col("payload.currency").alias("currency"),
    col("payload.status").alias("status")
)

#-----------------------------------------------
# 6. MERGE TO DELTA TABLE AND UPDATING REJECT TABLE
#-----------------------------------------------
# Repartitioning to reduce skewness
users_payload_df = users_payload_df.repartition(20, "event_id")
payments_payload_df = payments_payload_df.repartition(20, "event_id")
orders_payload_df = orders_payload_df.repartition(20, "event_id")

# Idempotent MERGE function
def merge_to_silver(df, table_name):
    delta_table = DeltaTable.forName(spark, table_name)
    (
        delta_table.alias("t").merge(
            df.alias("s"),
            "t.event_id = s.event_id"
        )
        .whenNotMatchedInsertAll()
        #.whenMatchedUpdateAll() - Use this for evolving schema but dangerous if it goes unmanaged
        .execute()
    )

# Executing MERGE
merge_to_silver(users_payload_df, "demo_catalog.silver.events_user")
merge_to_silver(payments_payload_df, "demo_catalog.silver.events_payment")
merge_to_silver(orders_payload_df, "demo_catalog.silver.events_order")
    
# Updating reject table
if rejected_dfs:
    final_rejections = reduce(lambda a, b: a.unionByName(b), rejected_dfs)
    final_rejections = final_rejections.persist()
    final_rejections.write.mode("append").saveAsTable("demo_catalog.bronze.rejected_events")
else:
    print("No rejections")

#-----------------------------------------------
# 7. METRIC COUNTS
#-----------------------------------------------
total_events = df_cont.groupBy("etag").agg(count(*)).alias("total_events")
rejected_events = final_rejections.groupBy("etag").agg(count(*)).alias("rejected_events")
metrics_df = total_events.join(rejected_events, "etag", "left").fillna(0)
metrics_df = metrics_df.withColumn(
                "valid_events", 
                col(total_events) - col(rejected_events)
)
metrics_df = metrics_df.withColumn(
                "file_status", 
                when(col("valid_events") == 0, "FAILED")
                .when(col("rejected_events") == 0, "SUCCESS")
                .otherwise("PARTIAL_SUCCESS")
)

etag_metrics = {
    row["etag"]: {
        "total": row["total_events"],
        "valid": row["valid_events"],
        "rejected": row["rejected_events"]
    }
    for row in metrics_df.collect()
}

df_cont.unpersist()
df_env.unpersist()

dbutils.notebook.exit(json.dumps({
    "status" : "COMPLETED",
    "metrics" : etag_metrics
}))