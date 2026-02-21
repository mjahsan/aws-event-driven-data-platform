from pyspark.sql.functions import *
from pysparl.sql.types import *
from delta.tables import DeltaTable
import json
import logging

#0. Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silver")

#1. Capturing file paths as one string string and converting it to a list
dbutils.widgets.text("file_paths", "")
file_paths = json.loads(dbutils.widgets.get("file_paths"))

if not file_paths:
    raise ValueError("No file paths provided to Silver job")

#2. Reading the JSON files from the list of paths
raw_df = spark.read.option("multiLine", True).json(file_paths)

#3. Basic file-level validation
required_cols = ["file_id, domain, source_system, created_at, events"]
missing_cols = [c for c in required_cols if c not in raw_df.columns]
if missing_cols:
    raise ValueError(f"Missing required columns:{missing_cols}")

#2. Flatten container
df_flat = df.select(
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

#4. Envelope-level validation data quality check:
df_env = df_env.filter(
    col("event_id").isNotNull() &
    col("event_type").isNotNull() &
    col("event_ts").isNotNull()
)
rejected_df_env = df_env.filter(
    col("event_id").isNull()
)
if rejected_df_env.count() > 0:
    logger.warning(f"Rejecting {rejected_df_env.count()} records due to missing event_id")

#5. Adding partition column after filtering non-null rows
df_env = df_env.withColumn("event_date", to_date(col("event_ts")))

#6. Splitting up by event type
orders_df = df_env.filter(col("event_type") == "order_events")
payment_df = df_env.filter(col("event_type") == "payment_events")
user_df = df_env.filter(col("event_type") == "user_events")

#7. Payload extraction per type
users_df = users_df.select(
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

payments_df = payments_df.select(
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
).filter((col("payment_id").isNotNull()) and (col("amount") >= 0))

orders_df = orders_df.select(
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

#8. Deduplication inside batch before MERGE to drop duplicated within a single file
users_df = users_df.dropDuplicates(["event_id"])
payments_df = payments_df.dropDuplicates(["event_id"])
orders_df = orders_df.dropDuplicates(["event_id"])

#9. Idempotent MERGE function
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
        .execute()
    )

#10. Executing MERGE
merge_to_silver(users_df, "demo.catalog.silver.events_user")
merge_to_silver(payments_df, "demo.catalog.silver.events_payments")
merge_to_silver(orders_df, "demo.catalog.silver.events_orders")
