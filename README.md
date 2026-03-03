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
    -   `VALIDATED → IN_PROGRESS → SUCCESS(SILVER)/PARTIAL_SUCCESS(SILVER)`
    -   On failure, `FAILED(SILVER)`: reverts to `VALIDATED`
	
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

## ⚙️ DEPLOYMENT & EXECUTION GUIDE

This project is a **manually provisioned reference architecture**.\
It is not packaged as a one-click deployment. Infrastructure must be
provisioned according to the provided configuration file.

Refer to: config/config.yml

### 1️. CONFIGURATION-DRIVEN SETUP

All components are defined inside:

`config.yml`

The configuration defines:

-   S3 bucket structure (raw, validated, rejected, silver, gold)
-   SQS queues and DLQs
-   Lambda functions
-	IAM roles and access policies
-   Step Functions definitions
-   DynamoDB table structure
-   Databricks job scripts and table definitions

Use this file as the authoritative blueprint for provisioning resources.

------------------------------------------------------------------------

### 2. INFRASTRUCTURE PROVISIONING (MANUAL)

Provision the following AWS resources:

-   S3 buckets and folder structure
-   SQS queues (with DLQ & visibility timeout)
-   DynamoDB table (`etag` as partition key, on-demand capacity)
-   Lambda functions (validation, job trigger, status check)
-   Step Functions (Silver orchestration, Gold orchestration)
-   EventBridge schedules for silver and gold step functions
-   AWS Secrets Manager (Databricks host + token)

Deploy Step Functions using the provided JSON definitions.\
Deploy Lambda functions using the referenced scripts.

------------------------------------------------------------------------

### 3. DATABRICKS SETUP

-	Create acess token in settings and store it along with the Databricks host in AWS Secrets Manager
-	Create IAM role in AWS as mentioned in the configuration
- 	Add Credentials using the IAM role's ARN in Unity Catalogue to generate trust policy. Attach the trust policy to the IAM role.
-	Create two job pipelines:

#### SILVER LAYER

-	Table definition script
-   PySpark transformation script
-   Delta MERGE logic
-   Z-ORDER optimization
-   Unity Catalog schema creation

### GOLD LAYER
-	Table definition script
-   SQL-based dimensional modeling
-   Fact tables (orders, payments)
-   Dimension tables (users)
-   Optimization scripts

------------------------------------------------------------------------

### 4. RUNNING THE PIPELINE

#### STEP 1 -- START SYNTHETIC PYTHON GENERATOR

Run:

python data_generator/generate_events.py

This generates 500 events per file every 5 seconds and uploads them to
S3 (raw layer).

------------------------------------------------------------------------

#### STEP 2 -- INGESTION FLOW

S3 → SQS → Lambda

Lambda performs: 

- 	JSON validation 
- 	File-level idempotency using DynamoDB 
- 	Routing to validated or rejected folders 
- 	Forwarding validated files to processing queue

------------------------------------------------------------------------

#### STEP 3 - SILVER PROCESSING (MICRO-BATCH)

EventBridge triggers Step Function every 5 minutes.

-   Collects up to 10 files
-   Triggers Databricks Silver job
-   Polls for status
-   Updates DynamoDB processing state

Silver performs: 

- 	Schema enforcement 
- 	Event-level idempotent MERGE 
- 	Rejected event capture 
- 	Delta optimization (Z-ORDER)

------------------------------------------------------------------------

#### STEP 4 -- GOLD PROCESSING

Triggered every 30--60 minutes.

-   Builds fact and dimension tables
-   Applies business transformations
-   Optimizes analytics-ready tables

------------------------------------------------------------------------

## 🔐 SECURITY AND GOVERNANCE

-   IAM roles with least privilege access (Refer to: infra/)
-   Secrets stored in AWS Secrets Manager
-   Databricks token securely retrieved by Lambda
-   Unity Catalog for governance
-   Controlled SQS visibility timeout
-   DLQ configuration
-   Retry & Catch mechanisms in Step Functions
-	idempotency and state control in DynamoDB

------------------------------------------------------------------------

## 🧠 DESIGN DECISIONS


| Component        | Rationale                          |
|------------------|------------------------------------|
| SQS              | Decoupling & buffering             |
| Lambda           | Lightweight validation             |
| DynamoDB         | Idempotency & state tracking       |
| Step Functions   | Reliable orchestration             |
| Databricks       | Distributed processing             |
| Delta Lake       | ACID + merge capability            |
| Micro-batching   | Throughput optimization            |

------------------------------------------------------------------------

## 📊 DATA FLOW SUMMARY

-	Python generator creates JSON files
-	Files uploaded to S3 (Raw)
-	SQS buffers events
-	Lambda validates + enforces idempotency at file level
-  	Valid files sent to validated folder
-  	Silver performs event-level merge on validated files + enforces idempotency at event level
-  	Gold aggregates into fact & dimension tables on silver tables

------------------------------------------------------------------------

## 🛠️ TECH STACK

-   AWS S3
-   AWS SQS
-   AWS Lambda
-   AWS DynamoDB
-   AWS Step Functions
-   AWS EventBridge
-   AWS Secrets Manager
-   Databricks
-   Delta Lake
-   PySpark
-   SQL
-   Python

------------------------------------------------------------------------

## 📈 KEY FEATURES

-   Fully event-driven
-   Serverless architecture
-   File-level idempotency
-   Event-level idempotency
-   Micro-batch processing
-   Retry & failure handling
-   Dead-letter queues
-   Lakehouse implementation
-   Fact & dimension modeling

------------------------------------------------------------------------

## 🚀 FUTURE ENHANCEMENTS

-   SNS failure notifications
-	Gold incremental inserts
-   Enhanced monitoring dashboards
-   Schema evolution handling
-   Gold layer job tracking in DynamoDB
-   CI/CD pipeline integration
