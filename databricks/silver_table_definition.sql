-- Creating external location for validated, bronze and silver with IAM role access
CREATE EXTERNAL LOCATION s3_validated_location
URL 's3://event-platform-new/validated'
WITH (CREDENTIAL `aws-databricks-connection-role`);

CREATE EXTERNAL LOCATION s3_bronze_location
URL 's3://event-platform-new/bronze'
WITH (CREDENTIAL `aws-databricks-connection-role`);

CREATE EXTERNAL LOCATION s3_silver_location
URL 's3://event-platform-new/silver'
WITH (CREDENTIAL `aws-databricks-connection-role`);

-- Catalog and schema creation
CREATE CATALOG IF NOT EXISTS demo_catalog;

USE CATALOG demo_catalog;

DROP SCHEMA IF EXISTS demo_catalog.silver CASCADE;
CREATE SCHEMA IF NOT EXISTS demo_catalog.silver;

DROP SCHEMA IF EXISTS demo_catalog.bronze CASCADE;
CREATE SCHEMA IF NOT EXISTS demo_catalog.bronze;

-- Granting access to catalog
GRANT USE CATALOG ON CATALOG demo_catalog TO `your-databricks-user`;

-- Granting access to validated (read intial validated data)
GRANT READ FILES ON EXTERNAL LOCATION s3_validated_location TO `your-databricks-user`;

-- Granting access to bronze (read and write for rejected data)
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA demo_catalog.bronze TO `your-databricks-user`;
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION s3_bronze_location TO `your-databricks-user`;

-- Granting access to silver (read and write for transformed data)
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA demo_catalog.silver TO `your-databricks-user`;
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION s3_silver_location TO `your-databricks-user`;

-- Silver tables creation
DROP TABLE IF EXISTS silver.events_user;
CREATE TABLE IF NOT EXISTS silver.events_user (
  file_id STRING,
  domain STRING,
  source_system STRING,
  created_at TIMESTAMP,
  event_id STRING, 
  event_type STRING, 
  source STRING,
  event_ts TIMESTAMP,
  ingest_ts TIMESTAMP,
  event_date DATE,
  user_id STRING,
  email STRING,
  country STRING,
  device STRING
)
USING DELTA
PARTITIONED BY (event_date)
LOCATION 's3://event-platform-new/silver/user_events';

DROP TABLE IF EXISTS silver.events_payment;
CREATE TABLE IF NOT EXISTS silver.events_payment(
  file_id STRING,
  domain STRING,
  source_system STRING,
  created_at TIMESTAMP,
  event_id STRING,
  event_type STRING, 
  source STRING,
  event_ts TIMESTAMP,
  ingest_ts TIMESTAMP,
  event_date DATE,
  payment_id STRING,
  order_id STRING,
  amount DECIMAL(10,2),
  currency STRING,
  failure_reason STRING
)
USING DELTA
PARTITIONED BY (event_date)
LOCATION 's3://event-platform-new/silver/payment_events';

DROP TABLE IF EXISTS silver.events_order;
CREATE TABLE IF NOT EXISTS silver.events_order (
  file_id STRING,
  domain STRING,
  source_system STRING,
  created_at TIMESTAMP,
  event_id STRING,  
  event_type STRING, 
  source STRING,
  event_ts TIMESTAMP,
  ingest_ts TIMESTAMP,
  event_date DATE,
  order_id STRING,
  user_id STRING,
  amount DECIMAL(10,2),
  currency STRING,
  status STRING
)
USING DELTA
PARTITIONED BY (event_date)
LOCATION 's3://event-platform-new/silver/order_events';

-- Bronze table creation for rejected entries
DROP TABLE IF EXISTS bronze.rejected_events;
CREATE TABLE IF NOT EXISTS bronze.rejected_events (
  file_id STRING,
  domain STRING,
  source_system STRING,
  created_at TIMESTAMP,
  etag STRING,
  event_id STRING, 
  event_type STRING, 
  rejection_reason STRING,
  raw_event_json STRING,
  rejection_ts TIMESTAMP,
  rejection_date DATE,
  reject_hash STRING
)
USING DELTA
PARTITIONED BY (rejection_date)
LOCATION 's3://event-platform-new/bronze/rejected_events';