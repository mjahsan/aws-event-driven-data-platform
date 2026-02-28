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
JOB_ID = 825662644725671 #os.environ["JOB_ID"]
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
        # Checking run status and retrieving metrics only for success runs
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

        if (life_cycle == "PENDING" or life_cycle == "RUNNING") and result_state is None:
            return {
                "run_id": run_id,
                 "overallstatus": "RUNNING"
            }

        else:
            if result_state != "SUCCESS":
                return {
                    "run_id": run_id,
                    "overallstatus": "FAILED"
                }
            else:
                # Run metrics
                metrics_response = http.request(
                    "GET",
                    f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get-output?run_id={task_run_id}",
                    headers=headers,
                    timeout = 10
                )
                if metrics_response.status != 200:
                    raise Exception(f"Failed to get run metrics: {metrics_response.data.decode()}")

                # Retrieving output data
                metrics_data = json.loads(metrics_response.data.decode())
                notebook_output = metrics_data.get("notebook_output")
                if not notebook_output or "result" not in notebook_output:
                    raise Exception(f"No Notebook found: {metrics_data}")
                result = notebook_output["result"]
                job_results = json.loads(result)
                overall_status = job_results.get("status")
                metrics = job_results.get("metrics")
                if not metrics:
                    raise Exception(f"No metrics found in notebook output: {job_results}")

                fileResults = [{"etag": etag, "status": metric['status'], "run_id": str(run_id)} for etag, metric in metrics.items()]

                return{
                    "overallstatus": overall_status,
                    "fileResults": fileResults                        
                }                

# This must raise so Step Function Retry works
    except Exception as e:
        raise Exception(f"Status Lambda Infra Failure: {str(e)}")
