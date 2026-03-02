import json
import os
import boto3
import urllib3
import logging

http = urllib3.PoolManager()
secrets_client = boto3.client("secretsmanager")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Retrieving Environment Variables
SECRET_NAME = os.environ["SECRET_NAME"]
JOB_ID = os.environ["JOB_ID"]
DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]

# Funstion to retrieve token from Secrets Manager
def get_token():
    response = secrets_client.get_secret_value(SecretId = SECRET_NAME)
    secret = json.loads(response['SecretString'])
    return secret['DATABRICKS_TOKEN']

def lambda_handler(event, context):
    try:
        token = get_token()
        logger.info("Successfully retrived the token from Secrets Manager")

        # Headers for HTTP request
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        # Payload for HTTP request
        payload = {
            "job_id": int(JOB_ID),
            "notebook_params": {
                "file_metadata": json.dumps(event)
            }
        }

        # Making POST request to Databricks Job Trigger EndPoint with timeout to prevent lonng running invocation.
        logger.info(f"Triggering Databricks Job ID: {JOB_ID}")
        response = http.request(
            'POST', 
            f"{DATABRICKS_HOST}/api/2.1/jobs/run-now", 
            body = json.dumps(payload), 
            headers = headers,
            timeout = 10
        )

        # Raising error to catch failure and enable Step Functions to retry
        if response.status != 200:
            raise Exception(f"Failed to trigger job. HTTP {response.status}: {response.data.decode()}")

        logger.info("Job triggered successfully")
        
        # Retrieving run_id from response for downstream state consumption
        data = json.loads(response.data.decode())
        run_id = data.get("run_id")
        logger.info(f"run_id retrieved successfully: {run_id}")
        if not run_id:
            raise Exception(f"No run_id returned: {data}")
        return {
            'run_id': run_id
        }

    except Exception as e:
        # This must raise so Step Function Retry works
        raise Exception(f"Trigger Lambda Infra Failure: {str(e)}")
