from pyspark.sql.functions import *
from pysparl.sql.types import *
from delta.tables import DeltaTable
import json

#-----------------------------------------------
# 1. PARAMETERS DECLARATION
#-----------------------------------------------
# Initial setup
# Use the following to enable schema evolution but it could go dangerous if unmanaged - better to raise error and fix schema as in #7
# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true") - # Used in MERGE function

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
rejected_container_df = raw_df.filer(
    col("file_id").isNull |
    col("domain").isNull |
    col("source_system").isNull |
    col("created_at").isNull
).select(
    col("file_id"),
    col("domain"),
    col("source_system"),
    col("created_at"),
    lit(None).alias("event_id"),
    lit(None).alias("event_type"),
    when(col("file_id").isNull, lit("MISSING_FILE_ID"))
        .when(col("domain").isNull, lit("MISSING_DOMAIN"))
        .when(col("source_system").isNull, lit("MISSING_SOURCE_SYSTEM"))
        .when(col("created_at").isNull, lit("MISSING_CREATION_TIME"))
        .alias("rejection_reason),
    to_json(struct("*")).alias("raw_event_json"),
    current_timestamp().alias("rejection_ts")
).withColumn("rejection_date", to_date(col("rejection_ts")))

if not rejected_container_df.isEmpty():
    rejected_df_env.write.mode("append").saveAsTable("demo_catalog.silver.rejected_events")

# Flatten approved container
df_cont = raw_df.filer(
    col("file_id").isNotNull &
    col("domain").isNotNull &
    col("source_system").isNotNull &
    col("created_at").isNotNull
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
rejected_env_df = df_flat.filter(
    col("event.event_id").isNull() |
    col("event.event_type").isNull() |
    col("event.event_ts").isNull() |
    col("event.source").isNull() |
    col("event.ingest_ts").isNull()
).select(
    col("file_id"),
    col("domain"),
    col("source_system"),
    col("created_at"),
    col("event.event_id").alias("event_id"),
    col("event.event_type").alias("event_type"),
    when(col("event_id").isNull, lit("MISSING_EVENT_ID"))
        .when(col("event_type").isNull, lit("MISSING_EVENT_TYPE"))
        .when(col("event_ts").isNull, lit("MISSING_EVENT_TIMESTAMP"))
        .when(col("source").isNull, lit("MISSING_SOURCE"))
        .when(col("ingest_ts").isNull, lit("MISSING_INGEST_TIMESTAMP"))
        .alias("rejection_reason),
    to_json(col("event")).alias("raw_event_json"),
    current_timestamp().alias("rejection_ts")
).withColumn("rejection_date", to_date(col("rejection_ts")))

if not rejected_env_df.isEmpty():
    rejected_env_df.write.mode("append").saveAsTable("demo_catalog.silver.rejected_events")

# Flatten approved envelope
df_env = df_cont.filter(
    col("event_id").isNotNull() &
    col("event_type").isNotNull() &
    col("event_ts").isNotNull()
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
)


# Adding partition column after filtering non-null rows
df_env = df_env.withColumn("event_date", to_date(col("event_ts")))

# Splitting up by event type
order_df = df_env.filter(col("event_type") == "order_events")
payment_df = df_env.filter(col("event_type") == "payment_events")
user_df = df_env.filter(col("event_type") == "user_events")

# Payload extraction per type along with the management of unexpected schema change
users_expected_df = user_df.filter(
    col("user_id").isNotNull()
).select(
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

expected_fields_user = {"user_id", "email", "country", "device"}
actual_fields_user = set(user_df.select("payload.*").columns)
unexpected_fields_user = actual_fields_user - expected_fields_user
if unexpected_fields_user:
    raise Exception (f"Unexpected payload fields detected: {unexpected_fields_user}")

payments_expected_df = payment_df.select(
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
).filter((col("payment_id").isNotNull()) & (col("amount") >= 0))

expected_fields_payment = {"payment_id", "order_id", "amount", "currency", "failure_reason"}
actual_fields_payment = set(payment_df.select("payload.*").columns)
unexpected_fields_payment = actual_fields_payment - expected_fields_payment
if unexpected_fields_payment:
    raise Exception (f"Unexpected payload fields detected: {unexpected_fields_payment}")

orders_expected_df = order_df.select(
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
).filter(col("order_id").isNotNull())

expected_fields_order = {"order_id", "user_id", "amount", "currency", "status"}
actual_fields_order = set(order_df.select("payload.*").columns)
unexpected_fields_order = actual_fields_order - expected_fields_order
if unexpected_fields_order:
    raise Exception (f"Unexpected payload fields detected: {unexpected_fields_order}")
    
# Deduplication inside batch before MERGE to drop duplicated within a single file
users_df = users_expected_df.dropDuplicates(["event_id"])
payments_df = payments_expected_df.dropDuplicates(["event_id"])
orders_df = orders_expected_df.dropDuplicates(["event_id"])

# Idempotent MERGE function
def merge_to_silver(df, table_name):
    if df.isEmpty():
        return
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
merge_to_silver(users_df, "demo_catalog.silver.events_user")
merge_to_silver(payments_df, "demo_catalog.silver.events_payment")
merge_to_silver(orders_df, "demo_catalog.silver.events_order")