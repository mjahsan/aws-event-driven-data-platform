-- Creating external location for silver and gold with IAM role access
CREATE EXTERNAL LOCATION s3_silver_location
URL 's3://event-platform-new/silver'
WITH (CREDENTIAL `aws-databricks-connection-role`);
-- Note that this 'silver' external location already exist and this role already has access to create table, read and write files in silver location which was granted during silver layer DDL.
-- If access restriction is needed for gold layer, a new IAM role and the carrying out the below steps would be required.

CREATE EXTERNAL LOCATION s3_gold_location
URL 's3://event-platform-new/gold'
WITH (CREDENTIAL `aws-databricks-connection-role`);

-- Schema creation
USE CATALOG demo_catalog;

DROP SCHEMA IF EXISTS demo_catalog.gold CASCADE;
CREATE SCHEMA IF NOT EXISTS demo_catalog.gold;

-- Granting access to catalog
GRANT USE CATALOG ON CATALOG demo_catalog TO `your-databricks-user`;

-- Granting access to silver (read access for silver data)
GRANT READ FILES ON EXTERNAL LOCATION s3_silver_location TO `your-databricks-user`;
-- Note that the above grant has no effect as the location and IAM already have access to read and write granted during silver layer DDL.

-- Granting access to silver (read and write for transformed data)
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA demo_catalog.gold TO `your-databricks-user`;
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION s3_gold_location TO `your-databricks-user`;

-- Gold tables creation
DROP TABLE IF EXISTS gold.fact_orders;
CREATE TABLE IF NOT EXISTS gold.fact_orders(
  order_id STRING, -- Primary Key
  user_id STRING, -- Foreign Key
  device_key INT, -- Foreign Key
  order_date DATE,
  order_amount DECIMAL (10,2),
  currency STRING,
  order_status STRING,
  is_paid_flag BOOLEAN,
  is_refunded_flag BOOLEAN
)
USING DELTA
PARTITIONED BY (order_date)
LOCATION 's3://event-platform-new/gold/fact_orders'

DROP TABLE IF EXISTS gold.fact_payments;
CREATE TABLE IF NOT EXISTS gold.fact_payments(
  payment_id STRING,-- Primary Key
  order_id STRING,-- Foreign Key
  payment_date DATE,
  payment_amount DECIMAL (10,2),
  currency STRING,
  payment_status STRING
)
USING DELTA
PARTITIONED BY (payment_date)
LOCATION 's3://event-platform-new/gold/fact_payments'

DROP TABLE IF EXISTS gold.dim_users;
CREATE TABLE IF NOT EXISTS gold.dim_users(
  user_id STRING,-- Primary Key
  email STRING,
  country STRING
)
USING DELTA
LOCATION 's3://event-platform-new/gold/dim_users'

DROP TABLE IF EXISTS gold.dim_devices;
CREATE TABLE IF NOT EXISTS gold.dim_devices(
  device_key INT,-- Primary Key
  device_type STRING
)
USING DELTA
LOCATION 's3://event-platform-new/gold/dim_devices'