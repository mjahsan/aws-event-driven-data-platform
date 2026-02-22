from pyspark.sql.functions import *
from pysparl.sql.types import *
from delta.tables import DeltaTable
import json

# Logging setup
# Use the following to enable schema evolution but it could go dangerous if unmanaged - better to raise error and fix schema as in #7
# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true") - # Used in MERGE function

#1. Capturing file paths as one string string and converting it to a list
dbutils.widgets.text("file_paths", "")
file_paths = json.loads(dbutils.widgets.get("file_paths"))

if not file_paths:
    raise ValueError("No file paths provided to Silver job")

# Reading the JSON files from the list of paths 
raw_df = spark.read.option("multiLine", True).json(file_paths)

# Basic file-level validation
required_cols = ["file_id", "domain", "source_system", "created_at", "events"]
missing_cols = [c for c in required_cols if c not in raw_df.columns]
if missing_cols:
    raise ValueError(f"Missing required columns:{missing_cols}")

# Flatten container
df_flat = raw_df.select(
    col("file_id"),
    col("domain"),
    col("source_system"),
    col("created_at"),
    explode("events").alias("event")
)

#3. Flatten event envelope
df_env = df_flat.select(
    "file_id",
    "domain",
    "source_system",
    "created_at",
    col("event.event_id").alias("event_id"),
    col("event.event_type").alias("event_type"),
    col("event.source").alias("source"),
    col("event.event_ts").alias("event_ts"),
    col("event.ingest_ts").alias("ingest_ts"),
    explode("event.payload").alias("payload")
)

# Extracting records with missing values - Rejected Records
rejected_df_env = df_flat.filter(
    col("event.event_id").isNull() |
    col("event.event_type").isNull() |
    col("event.event_ts").isNull()
).select(
    col("file_id"),
    col("domain"),
    col("source_system"),
    col("created_at"),
    col("event.event_id"),
    col("event.event_type"),
    col("event.source"),
    col("event.event_ts"),
    col("event.ingest_ts"),
    lit("MISSING_REQUIRED_ENVELOPE_FIELDS").alias("rejecion_reason"),
    to_json(col("payload")).alias("raw_event_json"),
    current_timestamp().alias("rejection_ts")
).withColumn("rejection_date", to_date(col("rejection_ts")))

if not rejected_df_env.isEmpty():
    rejected_df_env.write.mode("append").saveAsTable("demo_catalog.silver.rejected_events")

# Flatten event envelope
df_env = df_flat.select(
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

# Envelope-level validation data quality check:
df_valid_env = df_env.filter(
    col("event_id").isNotNull() &
    col("event_type").isNotNull() &
    col("event_ts").isNotNull()
)

# Adding partition column after filtering non-null rows
df_env = df_valid_env.withColumn("event_date", to_date(col("event_ts")))

# Splitting up by event type
order_df = df_env.filter(col("event_type") == "order_events")
payment_df = df_env.filter(col("event_type") == "payment_events")
user_df = df_env.filter(col("event_type") == "user_events")

# Payload extraction per type along with the management of unexpected schema change
users_expected_df = user_df.select(
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
).filter(col("user_id").isNotNull())

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