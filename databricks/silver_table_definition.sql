CREATE CATALOG IF NOT EXISTS demo_catalog;

USE demo_catalog;

CREATE SCHEMA IF NOT EXISTS demo_catalog.silver;

CREATE TABLE IF NOT EXISTS silver.events_user (
  file_id STRING,
  domain STRING,
  source_system STRING,
  created_at TIMESTAMP,
  event_id STRING PRIMARY KEY, #Primary key is only for decorative purpose and optional as delta does not enforce it 
  event_type STRING, 
  source STRING,
  event_ts TIMESTAMP,
  ingest_ts TIMESTAMP,
  user_id STRING,
  email STRING,
  country STRING,
  device STRING
  );

  CREATE TABLE IF NOT EXISTS silver.events_payment (
  file_id STRING,
  domain STRING,
  source_system STRING,
  created_at TIMESTAMP,
  event_id STRING PRIMARY KEY, #Primary key is only for decorative purpose and optional as delta does not enforce it 
  event_type STRING, 
  source STRING,
  event_ts TIMESTAMP,
  ingest_ts TIMESTAMP,
  payment_id STRING,
  order_id STRING,
  amount DECIMAL(10,2),
  currency STRING,
  failure_reason STRING
  );

  CREATE TABLE IF NOT EXISTS silver.events_order (
  file_id STRING,
  domain STRING,
  source_system STRING,
  created_at TIMESTAMP,
  event_id STRING PRIMARY KEY, #Primary key is only for decorative purpose and optional as delta does not enforce it 
  event_type STRING, 
  source STRING,
  event_ts TIMESTAMP,
  ingest_ts TIMESTAMP,
  order_id STRING,
  user_id STRING,
  amount DECIMAL(10,2),
  currency STRING,
  status STRING
  );
