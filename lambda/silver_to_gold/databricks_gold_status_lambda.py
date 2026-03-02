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
SECRET_NAME = os.environ['SECRET_NAME']
GOLD_JOB_ID = os.environ["GOLD_JOB_ID"]
DATABRICKS_HOST = os.environ['DATABRICKS_HOST']

# Function to retrieve token from Secrets Manager
def get_token():
    response = secrets_client.get_secret_value(SecretId = SECRET_NAME)
    secret = json.loads(response['SecretString'])
    return secret['DATABRICKS_TOKEN']

def lambda_handler(event, context):
    try:
        token = get_token()
        run_id = event['run_id']
        logger.info("Successfully retrived the token from Secrets Manager")

        # Header for HTTP request
        headers = {
            "Authorization": f"Bearer {token}"
        }
        #-------------------------------------------------------------------
        # Checking run status
        #-------------------------------------------------------------------
        # Run status
        status_response = http.request(
            "GET", 
            f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get?run_id={run_id}", 
            headers=headers,
            timeout = 10
        )
        
        if status_response.status != 200:
            raise Exception(f"Failed to get run status: {status_response.data.decode()}")

        # Retrieving task id for run metrics
        status_response = json.loads(status_response.data.decode())
        tasks = status_response.get("tasks")
        if tasks:
            task_run_id = tasks[0].get("run_id")
        else:
            task_run_id = run_id

        logger.info(f"Task run id: {task_run_id}")
        logger.info (f"Successfully retrived status data for {task_run_id}")

        # Retrieving run result
        life_cycle = status_response['state'].get("life_cycle_state")
        result_state = status_response['state'].get("result_state")
        print(life_cycle)
        print(result_state)

        # If job still running
        if life_cycle_state in ["PENDING", "RUNNING", "BLOCKED"]:
            return { "status": "RUNNING" }

        # If job finished successfully
        if life_cycle_state == "TERMINATED" and result_state == "SUCCESS":
            return { "status": "SUCCESS" }

        # Any other terminal state = failure
        if life_cycle_state == "TERMINATED" and result_state != "SUCCESS":
            return { "status": "FAILED" }

        # Safety fallback
        return { "status": "FAILED" }