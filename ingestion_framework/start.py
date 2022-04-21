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


def start(payload: dict):
    conn = ConnectionFactory(ConnectionType.DYNAMODB).get_connection().connect()

    transaction_id = str(uuid.uuid4())
    
    #Add workflow
    workflow_name = payload["data"]["wf_name"]
    print("Starting Workflow {}".format(workflow_name))

    wf_config_details = WorkFlowConfigDAO().read_record(record = {"key":{"partition_key":workflow_name}}, connector = conn)
    param_value = ""
    for wf_config in wf_config_details:
        if wf_config["param_name"]=="engine_type" and wf_config["is_active"]==True:
            param_value = wf_config["param_value"]
    


    job_details = JobDAO().read_record(record = {"key":{"partition_key":workflow_name}}, connector = conn)
    job_config_details = JobConfigDAO().read_record(record = {"key":{"partition_key":workflow_name}}, connector = conn)
    job_param_details = JobParamDAO().read_record(record = {"key":{"partition_key":workflow_name}}, connector = conn)
    
    for job in job_details:
        if job["is_active"]==True:
            job_name = job["job_name"]
            job_priority = job.get("job_priority",1)
            job_uuid = job["job_uuid"]

            for job_param in job_param_details:
                if job_param["is_active"]==True:


            eng_type_wf = JobEngineType.get_instance(param_value)
            eng = EngineFactory(eng_type_wf).get_engine()

    run_id = eng.start(workflow_name)        



def trigger(payload: dict):
    #Create a command factory and start execution of onboarding using handler onboard function
    cf = CommandFactory(Command.START).handler(start)
    response = cf.execute(data=payload)

