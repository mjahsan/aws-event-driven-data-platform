from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import json

#-----------------------------------------------
# 1. PARAMETERS DECLARATION
#-----------------------------------------------
# Initial setup
# Use the following to enable schema evolution but it could go dangerous if unmanaged - better to raise error and fix schema as in #7
# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true") - # Used in MERGE function (Section 5)

# Capturing file paths as one string string and converting it to a list
dbutils.widgets.text("file_paths", "")
file_paths = json.loads(dbutils.widgets.get("file_paths"))

if not file_paths:
    raise ValueError("No file paths provided to Silver job")
    
#-----------------------------------------------
# 2. FILE-LEVEL VALIDATION AND PROCESSING
#-----------------------------------------------
# Reading the JSON files from the list of paths 
raw_df = spark.read.option("multiLine", True).json(file_paths)

# Basic file-level validation: Check for missing columns
required_cols = ["file_id", "domain", "source_system", "created_at", "events"]
missing_cols = [c for c in required_cols if c not in raw_df.columns]
if missing_cols:
    raise ValueError(f"Missing required columns:{missing_cols}")

# Basic file-level validation: Check for nulls and extracting them to rejected table
rejected_container_df = raw_df.filter(
    col("file_id").isNull() |
    col("domain").isNull() |
    col("source_system").isNull() |
    col("created_at").isNull() |
    col("events").isNull() |
    (size(col("events")) == 0)
).select(
    col("file_id"),
    col("domain"),
    col("source_system"),
    col("created_at"),
    lit(None).alias("event_id"),
    lit(None).alias("event_type"),
    concat_ws(
        ",",
        when(col("file_id").isNull(), lit("MISSING_FILE_ID")),
        when(col("domain").isNull(), lit("MISSING_DOMAIN")),
        when(col("source_system").isNull(), lit("MISSING_SOURCE_SYSTEM")),
        when(col("created_at").isNull(), lit("MISSING_CREATION_TIME")),
        when(col("events").isNull(), lit("MISSING_CREATION_TIME")),
        when(col("events").isNull() | (size(col("events")) == 0), lit("EMPTY_EVENTS"))
    ).alias("rejection_reason"),
    to_json(struct("*")).alias("raw_event_json"),
    current_timestamp().alias("rejection_ts")
).withColumn("rejection_date", to_date(col("rejection_ts")))

if not rejected_container_df.isEmpty():
    rejected_container_df.write.mode("append").saveAsTable("demo_catalog.bronze.rejected_events")

# Flatten approved container
df_cont = raw_df.filter(
    col("file_id").isNotNull() &
    col("domain").isNotNull() &
    col("source_system").isNotNull() &
    col("created_at").isNotNull()
).select(
    col("file_id"),
    col("domain"),
    col("source_system"),
    col("created_at"),
    explode("events").alias("event")
)

#-----------------------------------------------
# 3. ENVELOPE-LEVEL VALIDATION AND PROCESSING
#-----------------------------------------------
# Basic envelope-level validation: Check for nulls and extracting them to rejected table
rejected_env_df = df_cont.filter(
    col("event.event_id").isNull() |
    col("event.event_type").isNull() |
    col("event.event_ts").isNull() |
    col("event.source").isNull() |
    col("event.ingest_ts").isNull() |
    col("event.ingest_ts") < col ("event.event_ts") |
    col("event.event_ts") > current_timestamp()
).select(
    col("file_id"),
    col("domain"),
    col("source_system"),
    col("created_at"),
    col("event.event_id").alias("event_id"),
    col("event.event_type").alias("event_type"),
    concat_ws(
        ",",
        when(col("event.event_id").isNull(), lit("MISSING_EVENT_ID")),
        when(col("event.event_type").isNull(), lit("MISSING_EVENT_TYPE")),
        when(col("event.event_ts").isNull(), lit("MISSING_EVENT_TIMESTAMP")),
        when(col("event.source").isNull(), lit("MISSING_SOURCE")),
        when(col("event.ingest_ts").isNull(), lit("MISSING_INGEST_TIMESTAMP")),
        when(col("event.ingest_ts") < col ("event.event_ts"), lit("EVENT_IS_GREATER_TO_INGEST_TIMESTAMP")),
        when(col("event.event_ts") > current_timestamp(), lit("EVENT_TIMESTAMP_IN_FUTURE")),
    ).alias("rejection_reason"),
    to_json(col("event")).alias("raw_event_json"),
    current_timestamp().alias("rejection_ts")
).withColumn("rejection_date", to_date(col("rejection_ts")))

if not rejected_env_df.isEmpty():
    rejected_env_df.write.mode("append").saveAsTable("demo_catalog.bronze.rejected_events")

# Flatten approved envelope
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

# Handling domain mismatch
domain_mismatch_df = df_env.filter(
    col("domain") != col("event_type")
).select(
    col("file_id"),
    col("domain"),
    col("source_system"),
    col("created_at"),
    col("event_id"),
    col("event_type"),
    lit("DOMAIN_EVENT_TYPE_MISMATCH").alias("rejection_reason"),
    to_json(struct("event_id", "event_type", "source", "event_ts","ingest_ts","payload")).alias("raw_event_json"),
    current_timestamp().alias("rejection_ts")
).withColumn("rejection_date", to_date(col("rejection_ts")))

if not domain_mismatch_df.isEmpty():
    domain_mismatch_df.write.mode("append").saveAsTable("demo_catalog.bronze.rejected_events")

df_env = df_env.filter(col("domain") == col("event_type"))

# Handling invalid event_type
valid_types = ["user_events", "payment_events", "order_events"]

invalid_type_df = df_env.filter(
    ~col("event_type").isin(valid_types)
).select(
    col("file_id"),
    col("domain"),
    col("source_system"),
    col("created_at"),
    col("event_id"),
    col("event_type"),
    lit("INVALID_EVENT_TYPE").alias("rejection_reason"),
    to_json(struct("event_id", "event_type", "source", "event_ts","ingest_ts","payload")).alias("raw_event_json"),
    current_timestamp().alias("rejection_ts")
).withColumn("rejection_date", to_date(col("rejection_ts")))

if not invalid_type_df.isEmpty():
    invalid_type_df.write.mode("append").saveAsTable("demo_catalog.bronze.rejected_events")

df_env = df_env.filter(col("event_type").isin(valid_types))

# Splitting up by event type
user_df = df_env.filter(col("event_type") == "user_events")
payment_df = df_env.filter(col("event_type") == "payment_events")
order_df = df_env.filter(col("event_type") == "order_events")

#-----------------------------------------------
# 4. DOMAIN-LEVEL VALIDATION AND PROCESSING
#-----------------------------------------------
# Handling schema evolution
if user_df.limit(1).count() > 0:
    users_expected_fields = {"user_id", "email", "country", "device"}
    users_actual_fields = set(users_df.select("payload.*").columns)
    users_unexpected_fields = users_actual_fields - users_expected_fields
if users_unexpected_fields:
    raise Exception (f"Unexpected field(s) detected: {users_unexpected_fields}")

if payment_df.limit(1).count() > 0:
    payments_expected_fields = {"payment_id", "order_id", "amount", "currency", "failure_reason"}
    payments_actual_fields = set(payments_df.select("payload.*").columns)
    payments_unexpected_fields = payments_actual_fields - payments_expected_fields
if payments_unexpected_fields:
    raise Exception (f"Unexpected field(s) detected: {payments_unexpected_fields}")

if order_df.limit(1).count() > 0:
    orders_expected_fields = {"user_id", "order_id", "amount", "currency", "status"}
    orders_actual_fields = set(orders_df.select("payload.*").columns)
    orders_unexpected_fields = orders_actual_fields - orders_expected_fields
if orders_unexpected_fields:
    raise Exception (f"Unexpected field(s) detected: {orders_unexpected_fields}")

# Captruing null values and extracting them to rejected tables
users_rejected_df = users_df.filter(
    col("payload.user_id").isNull()
).select(
    col("file_id"),
    col("domain"),
    col("source_system"),
    col("created_at"),
    col("event_id"),
    col("event_type"),
    lit("MISSING_USER_ID").alias("rejection_reason"),
    to_json(col("payload")).alias("raw_event_json"),
    current_timestamp().alias("rejection_ts")
).withColumn("rejection_date", to_date(col("rejection_ts")))

if not users_rejected_df.isEmpty():
    users_rejected_df.write.mode("append").saveAsTable("demo_catalog.bronze.rejected_events")

payments_rejected_df = payments_df.filter(
    col("payload.payment_id").isNull() |
    col("payload.order_id").isNull() |
    col("payload.amount") <= 0
).select(
    col("file_id"),
    col("domain"),
    col("source_system"),
    col("created_at"),
    col("event_id"),
    col("event_type"),
    concat_ws(
        ",",
        when(col("payload.payment_id").isNull(), lit("MISSING_PAYMENT_ID")),
        when(col("payload.order_id").isNull(), lit("MISSING_ORDER_ID")),
        when(col("payload.amount") <= 0, lit("INVALID AMOUNT"))
    ).alias("rejection_reason"),
    to_json(col("payload")).alias("raw_event_json"),
    current_timestamp().alias("rejection_ts")
).withColumn("rejection_date", to_date(col("rejection_ts")))

if not payments_rejected_df.isEmpty():
    payments_rejected_df.write.mode("append").saveAsTable("demo_catalog.bronze.rejected_events")

orders_rejected_df = orders_df.filter(
    col("payload.order_id").isNull() |
    col("payload.user_id").isNull() |
    col("payload.amount") <= 0
).select(
    col("file_id"),
    col("domain"),
    col("source_system"),
    col("created_at"),
    col("event_id"),
    col("event_type"),
    concat_ws(
        ",",
        when(col("payload.user_id").isNull(), lit("MISSING_USER_ID")),
        when(col("payload.order_id").isNull(), lit("MISSING_ORDER_ID")),
        when(col("payload.amount") <= 0, lit("INVALID AMOUNT"))
    ).alias("rejection_reason"),
    to_json(col("payload")).alias("raw_event_json"),
    current_timestamp().alias("rejection_ts")
).withColumn("rejection_date", to_date(col("rejection_ts")))

if not orders_rejected_df.isEmpty():
    orders_rejected_df.write.mode("append").saveAsTable("demo_catalog.bronze.rejected_events")

# Payload extraction per type
users_payload_df = user_df.select(
    "file_id",
    "domain",
    "source_system",
    "created_at",
    "event_id",
    "event_type",
    "source",
    "event_ts",
    "ingest_ts",
    "event_date",
    col("payload.user_id").alias("user_id"),
    col("payload.email").alias("email"),
    col("payload.country").alias("country"),
    col("payload.device").alias("device")
)

payments_payload_df = payment_df.select(
    "file_id",
    "domain",
    "source_system",
    "created_at",
    "event_id",
    "event_type",
    "source",
    "event_ts",
    "ingest_ts",
    "event_date",
    col("payload.payment_id").alias("payment_id"),
    col("payload.order_id").alias("order_id"),
    col("payload.amount").cast("decimal(10,2)").alias("amount"),
    col("payload.currency").alias("currency"),
    col("payload.failure_reason").alias("failure_reason")
)

orders_payload_df = order_df.select(
    "file_id",
    "domain",
    "source_system",
    "created_at",
    "event_id",
    "event_type",
    "source",
    "event_ts",
    "ingest_ts",
    "event_date",
    col("payload.order_id").alias("order_id"),
    col("payload.user_id").alias("user_id"),
    col("payload.amount").cast("decimal(10,2)").alias("amount"),
    col("payload.currency").alias("currency"),
    col("payload.status").alias("status")
)

# Approved domain values
users_df = users_payload_df.filter(
    col("user_id").isNotNull()
).dropDuplicates(["event_id"])
payments_df = payments_payload_df.filter(
    col("payment_id").isNotNull() &
    col("order_id").isNotNull() &
    col("amount") > 0
).dropDuplicates(["event_id"])
orders_df = orders_payload_df.filter(
    col("order_id").isNotNull() &
    col("user_id").isNotNull() &
    col("amount") > 0
).dropDuplicates(["event_id"])
    
#-----------------------------------------------
# 5. MERGE TO DELTA TABLE
#-----------------------------------------------
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
if not users_df.rdd.isEmpty():
    merge_to_silver(users_df, "demo_catalog.silver.events_user")
if not payments_df.rdd.isEmpty():
    merge_to_silver(payments_df, "demo_catalog.silver.events_payment")
if not orders_df.rdd.isEmpty():
    merge_to_silver(orders_df, "demo_catalog.silver.events_order")
    
#-----------------------------------------------
# 5. METRIC COUNTS
#-----------------------------------------------
metrics = {
    "total_events": df_cont.count(),
    "valid_events": users_df.count() + payments_df.count() + orders_df.count(),
    "rejected_events": (
        rejected_container_df.count() +
        rejected_env_df.count() +
        users_rejected_df.count() +
        payments_rejected_df.count() +
        orders_rejected_df.count() +
        domain_mismatch_df.count() +
        invalid_type_df.count()
    )
}

if metrics["valid_events"] == 0:
    batch_status = "FAILED"
elif metrics["rejected_events"] == 0:
    batch_status = "SUCCESS"
else:
    batch_status = "PARTIAL_SUCCESS"
    
dbutils.notebook.exit(json.dumps({
    "status" : batch_status,
    "metrics" : metrics
}))