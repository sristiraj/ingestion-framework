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
    
    #workflow passed as param
    workflow_name = payload["data"]["wf_name"]
    #Check if existing run already ongoing for workflow
    run_status = RunStatusDAO()
    runs = run_status.read_record(record = {"key":{"partition_key":workflow_name}}, connector = conn)

    for run in runs:
        if run["job_status"] == "waiting" or run["job_status"] == "running":
            print("Run for workflow exists with status in waiting and running")
            return run["sort_key"]
    #Add workflow
    print("Starting Workflow {}".format(workflow_name))

    wf_config_details = WorkFlowConfigDAO().read_record(record = {"key":{"partition_key":workflow_name}}, connector = conn)

    wf_name= wf_config_details[0]["partition_key"]
    param_value = ""
    for wf_config in wf_config_details:
        if wf_config["param_name"]=="engine_type" and wf_config["is_active"]==True:
            param_value = wf_config["param_value"]
    


    job_details = JobDAO().read_record(record = {"key":{"partition_key":workflow_name}}, connector = conn)
    job_config_details = JobConfigDAO().read_record(record = {"key":{"partition_key":workflow_name}}, connector = conn)
    job_param_details = JobParamDAO().read_record(record = {"key":{"partition_key":workflow_name}}, connector = conn)
    
    job_inter = []
    for job in job_details:
        if job["is_active"]==True:
            job_name = job["job_name"]
            job_priority = job.get("job_priority",1)
            job_uuid = job["job_uuid"]
            #To be changed later
            job_arn = "generate_PDF"
            arguments = {"--enable-glue-datacatalog":""}
            for job_param in job_param_details:
                if job_param["is_active"]==True and job_param["job_uuid"]==job_uuid:
                    arguments["--"+job_param["param_name"]]=job_param["param_value"]
            job_inter.append({"wf_uuid":wf_name, "engine_type":param_value, "job_uuid":job_uuid, "job_name":job_name, "job_arn":job_arn, "job_priority":job_priority, "arguments":arguments})


    for job in job_inter:
        run_status.create_record({"partition_key":job["wf_uuid"], "wf_run_event_id":transaction_id, "sort_key":job["job_uuid"]+"~"+transaction_id, "run_id":transaction_id, "job_uuid":job["job_uuid"], "job_name":job["job_name"], "engine_type": job["engine_type"], "job_arn":job_arn, "priority":job["job_priority"], "job_status":"waiting", "arguments": job["arguments"], "run_start_time":datetime.now().isoformat(), "run_end_time":None, "created_date":datetime.now().isoformat(),"updated_date":datetime.now().isoformat()}, conn)
    
    return {"status":"submitted","wf_run_event_id": transaction_id}
    #Moved the trigger of actual job to monitor
    # for job in job_inter:
    #     eng_type_wf = JobEngineType.get_instance(param_value)
    #     eng = EngineFactory(eng_type_wf).get_engine()
    #     run_id = eng.start(job["job_arn"], dict(job["arguments"]))    
    #     run_status.update_record({"key":{"partition_key":job["wf_uuid"], "sort_key":job["job_uuid"]+"~"+transaction_id}, "update_item":[{"update_col": "run_status", "update_val":"running"},{"update_col": "run_id", "update_val":run_id},{"update_col": "run_start_time", "update_val":datetime.now().isoformat()}]},conn)    



def trigger(payload: dict):
    #Create a command factory and start execution of onboarding using handler onboard function
    cf = CommandFactory(Command.START).handler(start)
    response = cf.execute(data=payload)
    return response

