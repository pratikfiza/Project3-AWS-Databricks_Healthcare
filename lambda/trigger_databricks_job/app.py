"""
Lambda to trigger a Databricks job via REST API (jobs/run-now).
The Step Functions state machine invokes this lambda to run the ETL job.
"""

import os
import json
import requests

DATABRICKS_INSTANCE = os.getenv("DATABRICKS_INSTANCE")  # e.g. https://<aws-instance>.azuredatabricks.net
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
JOB_ID = os.getenv("DATABRICKS_JOB_ID")  # integer

def trigger_databricks_job(job_id, notebook_params=None):
    url = f"{DATABRICKS_INSTANCE}/api/2.1/jobs/run-now"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    payload = {"job_id": int(job_id)}
    if notebook_params:
        payload["notebook_params"] = notebook_params
    resp = requests.post(url, headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json()

def lambda_handler(event, context):
    # event may contain job_params or file_key to pass to notebook
    params = event.get("job_params", {})
    run_resp = trigger_databricks_job(JOB_ID, notebook_params=params)
    return {"status": "triggered", "response": run_resp}
