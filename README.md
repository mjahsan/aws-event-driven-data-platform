# 🚀 EVENT-DRIVEN LAKEHOUSE DATA PLATFORM WITH AWS AND DATABRICKS

## 📌 OVERVIEW

This project implements a fully event-driven, cloud-native data platform
on AWS, integrated with Databricks for scalable data processing and
transformation.

The architecture follows a modern **Data Lakehouse paradigm**,
implementing Bronze (Raw), Silver (Validated & Cleaned), and Gold
(Business-Ready) layers with strong idempotency, orchestration, and
micro-batching strategies.

------------------------------------------------------------------------

## 🏗️ HIGH LEVEL ARCHITECTURE

**Core Principles:** 

- 	Event-driven ingestion 
- 	Micro-batch orchestration 
- 	Idempotent processing (file-level + event-level) 
-	Scalable serverless architecture 
-	Lakehouse architecture using Delta Lake 
-	Separation of ingestion and transformation planes

------------------------------------------------------------------------

## 🔹 DATA INGESTION PLANE

------------------------------------------------------------------------

### 1️. SYNTHETIC EVENT GENERATOR (PYTHON)

-   Generates synthetic:
    -   Order Events
    -   Payment Events
    -   User Events
-   500 events per file
-   New file generated every 5 seconds
-   Includes:
    -   Valid structured JSON
    -   Malformed JSON
    -   Truncated records
    -   Duplicates
    -   Messy real-world scenarios

Output: JSON files uploaded to **S3 (Raw Layer)**

------------------------------------------------------------------------

### 2️. AMAZON S3 (RAW LAYER)

-   Stores all generated JSON files
-   Acts as initial landing zone and source of truth
-   Triggers SQS via event notifications

------------------------------------------------------------------------

### 3️. AWS SQS (STANDARD QUEUE - BUFFER LAYER)

-   Buffers incoming files
-   Decouples ingestion from validation
-   Configured with:
    -   Visibility timeout aligned with downstream processing
    -   Dead Letter Queue (DLQ)
    -   Retry policy (3 retries)

------------------------------------------------------------------------

### 4️. AWS LAMBDA -  VALIDATION AND IDEMPOTENCY

#### File-Level Idempotency (DynamoDB)

-   DynamoDB Table:
    -   Partition Key: `eTag`
    -   Capacity Mode: On-Demand
    -   No GSI / LSI
-   Lambda checks if file eTag already exists
-   Prevents duplicate file processing

#### JSON Validation

-   Accepts only valid JSON
-   Malformed / corrupted files rejected

At this stage, ingestion is complete.

------------------------------------------------------------------------

## 🔹 CONTROL & TRANSFORMATION PLANE

------------------------------------------------------------------------

### 🥈 SILVER LAYER (DATABRICKS - PYSPARK)

#### 1. EVENTBRIDGE SCHEDULER

-   Triggers Step Function every 5 minutes
-   Enables controlled micro-batch ingestion

#### 2. AWS STEP FUNCTIONS ORCHESTRATION

-  	Pulls messages from Processing SQS
-  	Waits until 10 files accumulate (micro-batch strategy) or for 5 seconds
-  	Triggers Lambda → Retrieves Databricks credentials from Secrets Manager → Calls Databricks REST API for Silver Transformation
-  	Retrieves `run_id`
-  	Polls job status via another Lambda
-  	Updates DynamoDB status:
    -   `validated → in_progress → success`
    -   On failure: reverts to `validated`
	
Refer to: 
	-	.png: step_functions/validated_to_silver_state_machine.png
	-	.json: step_functions/validated_to_silver_state_machine.json

------------------------------------------------------------------------

#### 3. DATABRICKS SILVER TRANSFORMATION

##### Technologies:

-   PySpark
-   Delta Lake
-   Unity Catalog
-   S3-backed storage

##### Features:

-   Schema enforcement
-   Table definitions
-	External locations declarations
-	Permission grant to specific folders and tables for read/write access
-   Catalog creation
-   Event-level idempotency using:
    ``` sql
    MERGE INTO target
    WHEN NOT MATCHED THEN INSERT
    ```
-   Rejected event capture:
    -   Missing primary keys
    -   Schema mismatch
    -   Structural issues
	- 	Domain mismatch

Refer to: databricks/silver

##### Techniques/Optimizations:

-   Delta Lake OPTIMIZE
-   Z-ORDER
-   Persistence strategies
-	VACUUM

Refer to: databricks/silver/silver_layer_performance_architecture.md

Output: Cleaned, conformed Silver tables stored in S3

------------------------------------------------------------------------

### 🥇 GOLD LAYER (DATABRICKS - SQL)

#### 4. EVENTBRIDGE SCHEDULER

-   Scheduled every 30--60 minutes to triggers Step Function (gold)
-   Enables gold table refresh as per business use case

#### 5. AWS STEP FUNCTIONS ORCHESTRATION

-   Separate Step Function to decouple gold plane from silver
-   Scheduled every 30--60 minutes
-	Triggers Lambda → Retrieves Databricks credentials from Secrets Manager → Calls Databricks REST API for Silver Transformation
-	Retrieves `run_id`
-	Polls job status via another Lambda

Refer to: 
	-	.png: step_functions/silver_to_gold_state_machine.png
	-	.json: step_functions/silver_to_gold_state_machine.json

------------------------------------------------------------------------

#### 6. DATABRICKS GOLD TRANSFORMATION

###### Technologies:

-   SQL
-   Delta Lake
-   Unity Catalog
-   S3-backed storage

##### Features:

-	External locations declarations
-	Permission grant to specific folders and tables for read/write access
-   Catalog creation

Refer to: databricks/gold

##### Transformation Strategy

Implemented dimensional modeling:

##### Fact Tables:

-   Orders
-   Payments

##### Dimension Tables:

-   Users

##### Techniques/Optimizations:

-   SQL-based transformations
-	Full refresh
-   Business logic application
-   Z-ORDER optimization

Produces analytics-ready datasets for BI / reporting.

------------------------------------------------------------------------

## 📁 REPOSITORY STRUCTURE

```text
├── README.md
├── config
│   └── config.yml
├── data_flow_diagram.png
├── data_generator
│   ├── config.yaml
│   ├── generate_events.py
│   └── schemas
│       ├── event_envelope_schema.json
│       ├── file_container_schema.json
│       ├── order_payload_schema.json
│       ├── payment_payload_schema.json
│       └── user_payload_schema.json
├── databricks
│   ├── gold
│   │   ├── gold_maintenance_job.sql
│   │   ├── gold_table_definition.sql
│   │   └── gold_transformation_job.sql
│   └── silver
│       ├── silver_layer_performance_architecture.md
│       ├── silver_maintenance_job.sql
│       ├── silver_table_definition.sql
│       └── silver_transformation_job.py
├── infra
│   ├── iam
│   │   ├── databricks
│   │   │   ├── db_aws_connection_role.json
│   │   │   └── db_aws_connection_trust_policy.json
│   │   ├── event_bridge
│   │   │   ├── eb_silver_to_gold_policy.json
│   │   │   └── eb_validated_to_silver_policy.json
│   │   ├── lambda
│   │   │   ├── lambda_execution_policy.json
│   │   │   └── lambda_secrets_manager_policy.json
│   │   └── step_functions
│   │       ├── sf_silver_to_gold_policy.json
│   │       └── sf_validated_to_silver_policy.json
│   └── sqs
│       ├── s3_to_validation_lambda
│       │   ├── dlq_for_s3_to_validation_lambda.json
│       │   └── s3_to_validation_lambda.json
│       └── validation_lambda_to_step_functions
│           ├── dlq_for_validation_lambda_to_step_functions.json
│           └── validation_lambda_to_step_functions.json
├── lambda
│   ├── raw_to_validated_or_rejected
│   │   └── event_processing_function.py
│   ├── silver_to_gold
│   │   ├── databricks_gold_status_lambda.py
│   │   └── databricks_gold_trigger_lambda.py
│   └── validated_to_silver
│       ├── databricks_silver_status_lambda.py
│       └── databricks_silver_trigger_lambda.py
├── requirements.txt
└── step_functions
    ├── silver_to_gold_state_machine.json
    ├── silver_to_gold_state_machine.png
    ├── validated_to_silver_state_machine.json
    └── validated_to_silver_state_machine.png
```

------------------------------------------------------------------------