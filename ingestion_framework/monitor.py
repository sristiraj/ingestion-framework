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
def monitor(payload: dict):
    start = time.time()
    conn = ConnectionFactory(ConnectionType.DYNAMODB).get_connection().connect()

    transaction_id = str(uuid.uuid4())
    
    #workflow passed as param
    workflow_name = payload["data"]["wf_name"]
    #Check if existing run already ongoing for workflow
    job_inter = []
    run_status = RunStatusDAO()
    runs = run_status.read_record(record = {"key":{"partition_key":workflow_name}}, connector = conn)
    for run in runs:
        if run["job_status"] == "waiting" or run["job_status"] == "running":
            job_priority = run.get("job_priority",1)
            engine_type = run["engine_type"]
            #To be changed later
            job_arn = run["job_arn"]

            arguments = run["arguments"]
            arguments["--enable-glue-datacatalog"] = ""
            sort_key = run["sort_key"]
            job_name = run["job_name"]
            job_uuid = run["job_uuid"]
            job_inter.append({"partition_key":workflow_name, "sort_key": sort_key, "job_arn":run["job_arn"], "job_uuid": job_uuid, "job_name":job_name, "engine_type":engine_type, "job_priority": job_priority, "arguments": run["arguments"], "job_status":run["job_status"], "run_id": run["run_id"]})
    #Add workflow
    print("Monitoring Workflow {}".format(workflow_name))
    counter = 0
    job_loop = []
    for ct in range(len(job_inter)):
        if job_inter[ct]["job_status"]  == "waiting":
            eng_type_wf = JobEngineType.get_instance(job_inter[ct]["engine_type"])
            eng = EngineFactory(eng_type_wf).get_engine()
            run_id = eng.start(job_inter[ct]["job_arn"], dict(job_inter[ct]["arguments"]))  
            job_det = job_inter[ct]
            job_det["run_id"]=run_id
            job_det["partition_key"]=job_inter[ct]["partition_key"]
            job_det["sort_key"]=job_inter[ct]["sort_key"]
            job_det["job_status"]="running"
            run_status.update_record({"key":{"partition_key":job_inter[ct]["partition_key"], "sort_key":job_inter[ct]["sort_key"]}, "update_item":[{"update_col": "job_status", "update_val":"running"},{"update_col": "run_id", "update_val":run_id},{"update_col": "run_start_time", "update_val":datetime.now().isoformat()}]},conn)    
            job_loop.append(job_det)
        elif  job_inter[ct]["job_status"]  == "running": 
            job_det = job_inter[ct]
            # job_det["run_id"]=run_id
            job_det["partition_key"]=job_inter[ct]["partition_key"]
            job_det["sort_key"]=job_inter[ct]["sort_key"]
            job_det["job_status"]="running"
            job_loop.append(job_det)

    while True:
        counter = 0
        error_counter = 0
        for running_job in job_loop:
            if running_job["job_status"]=="running":
                eng_type_wf = JobEngineType.get_instance(running_job["engine_type"])
                eng = EngineFactory(eng_type_wf).get_engine()
                status = eng.monitor(running_job["job_arn"], running_job["run_id"])
                logger.info(running_job)
                if status == "running":
                    counter = counter + 1 
                if status == "completed":
                    run_status.update_record({"key":{"partition_key":running_job["partition_key"], "sort_key":running_job["sort_key"]}, "update_item":[{"update_col": "job_status", "update_val":status},{"update_col": "run_end_time", "update_val":datetime.now().isoformat()}]},conn)    
                if status == "error":
                    error_counter = error_counter + 1 
                    run_status.update_record({"key":{"partition_key":running_job["partition_key"], "sort_key":running_job["sort_key"]}, "update_item":[{"update_col": "job_status", "update_val":status},{"update_col": "run_end_time", "update_val":datetime.now().isoformat()}]},conn)    

        done = time.time()
        elapsed = done - start
        #check for lambda timeout
        
        if elapsed > 840:
            break
        
        
        if counter == 0:
            break

        time.sleep(SLEEP_TIME)

    if error_counter > 0:
        counter = -1
    
    if elapsed > 840:
        counter = 1 
    
    if counter > 0:
        counter = 1
    
    print({"result":str(counter)})    
    return dict({"result":str(counter)})
        
def trigger(payload: dict):
    #Create a command factory and start execution of onboarding using handler onboard function
    cf = CommandFactory(Command.START).handler(monitor)
    response = cf.execute(data=payload)
    print(response)
    return response

