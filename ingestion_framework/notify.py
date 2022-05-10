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
def notify(payload: dict):
    conn = ConnectionFactory(ConnectionType.DYNAMODB).get_connection().connect()
    
    #workflow passed as param
    workflow_name = payload["data"]["wf_name"]
    wf_run_event_id = payload["data"]["wf_run_event_id"]
    #Check if existing run already ongoing for workflow
    job_inter = []
    run_status = RunStatusDAO()
    runs = run_status.read_record(record = {"key":{"partition_key":workflow_name}}, connector = conn)
    job_loop = []
    job_det = {}
    for run in runs:
        job_det = {}
        if run["wf_run_event_id"] == wf_run_event_id:
            job_det["wf_run_event_id"] = wf_run_event_id
            job_det["wf_name"] = run["wf_name"]
            job_det["run_id"] = run["run_id"]
            job_det["run_status"] = run["run_status"]
            job_loop.append(job_det)
    
def trigger(payload: dict):
    #Create a command factory and start execution of onboarding using handler onboard function
    cf = CommandFactory(Command.NOTIFY).handler(notify)
    response = cf.execute(data=payload)
    return response

