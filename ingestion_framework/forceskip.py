import os, sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from ingestion_framework.util.command import CommandFactory
from ingestion_framework.util.command import Command
from ingestion_framework.core.dao import *
from ingestion_framework.core.dao.RunStatusDAO import RunStatusDAO
from ingestion_framework.core.dao.WorkFlowConfigDAO import WorkFlowConfigDAO
from ingestion_framework.core.dao.JobDAO import JobDAO
from ingestion_framework.core.dao.JobConfigDAO import JobConfigDAO
from ingestion_framework.core.dao.JobParamDAO import JobParamDAO
from ingestion_framework.core.engine import *
from ingestion_framework.core.connection import *
import uuid
import logging
import time


logger = logging.getLogger(__file__)
#Time to sleep between monitor checks in secs
SLEEP_TIME = 10
def forceskip(payload: dict):
    start = time.time()
    conn = ConnectionFactory(ConnectionType.DYNAMODB).get_connection().connect()
    print(payload)
    transaction_id = str(uuid.uuid4())
    
    #workflow passed as param
    workflow_name = payload["data"]["wf_name"]
    #Check if existing run already ongoing for workflow
    job_inter = []
    run_status = RunStatusDAO()
    runs = run_status.read_record(record = {"key":{"partition_key":workflow_name}}, connector = conn)
    print(runs)
    for run in runs:
        if run["job_status"] == "error" or run["job_status"] == "waiting":
            print(run["sort_key"])
            run_status.update_record({"key":{"partition_key":workflow_name, "sort_key":run["sort_key"]}, "update_item":[{"update_col": "job_status", "update_val":"forceskip"},{"update_col": "run_end_time", "update_val":datetime.now().isoformat()}]},conn)


    return dict({"wf_name":workflow_name, "status":"forceskipped"})

        
def trigger(payload: dict):
    #Create a command factory and start execution of onboarding using handler onboard function
    cf = CommandFactory(Command.MONITOR).handler(forceskip)
    response = cf.execute(data=payload)
    print(response)
    return response

