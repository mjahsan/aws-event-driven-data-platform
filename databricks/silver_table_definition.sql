CREATE CATALOG IF NOT EXISTS demo_catalog;

USE demo_catalog;

CREATE SCHEMA IF NOT EXISTS demo_catalog.silver;

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
;

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
PARTITIONED BY (event_date);

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
PARTITIONED BY (event_date);
