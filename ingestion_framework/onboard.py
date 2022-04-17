import os, sys
sys.path.append(os.path.dirname(__file__))

from util.command import CommandFactory
from util.command import Command
from core.dao import *
from core.dao.DataSetDAO import DataSetDAO
from core.dao.WorkFlowDAO import WorkFlowDAO
from core.dao.WorkFlowConfigDAO import WorkFlowConfigDAO
from core.dao.JobDAO import JobDAO
from core.dao.JobConfigDAO import JobConfigDAO
from core.dao.JobParamDAO import JobParamDAO
from core.connection import *
import uuid


def onboard(payload: dict):
    conn = ConnectionFactory(ConnectionType.DYNAMODB).get_connection().connect()
    transaction_id = str(uuid.uuid4())
    #Add workflow
    workflow_name = payload["data"]["wf_name"]
    engine = payload["data"]["engine_type"]
    template = payload["data"].get("job_template",None)
    sys_config = payload["data"].get("system_config_table",None)
    wf_config = payload["data"].get("wf_config",None)

    wf_uuid = WorkFlowDAO().create_record({"partition_key":workflow_name, "sort_key": "true", "wf_uuid":str(uuid.uuid4()),"wf_name":workflow_name,"job_schedule_uuid":None,"is_active":True,"created_date":datetime.now().isoformat(),"updated_date":datetime.now().isoformat(),"version":1,"transaction_id":transaction_id}, conn)
    

    # workflow config
    wfc = WorkFlowConfigDAO()
    old_version = 0
    #Read old records
    record_wfc = wfc.read_record(record = {"key":{"partition_key":workflow_name}}, connector = conn)
    #Inactivate old records
    if len(record_wfc)>0:
        for itm in record_wfc:
            old_version = itm["version"] if itm["version"]>old_version else old_version

            if itm["is_active"] is True:
                wfc.update_record({"key":{"partition_key":workflow_name, "sort_key":itm["sort_key"]}, "update_item":[{"update_col": "is_active", "update_val":False}]}, conn)

    #Create new records for wf config
    new_version = int(old_version)+1

    for item in wf_config.items():
        wf_config_uuid = wfc.create_record({"partition_key":workflow_name, "sort_key":str(True)+"~wf_config~"+item[0]+"~"+str(new_version), "wf_config_uuid":str(uuid.uuid4()), "wf_uuid":wf_uuid, "param_name":item[0], "param_value":item[1], "version":new_version, "is_active":True,"created_date":datetime.now().isoformat(),"updated_date":datetime.now().isoformat(),"transaction_id":transaction_id}, conn)
    
    if template is not None:
        wf_config_uuid = wfc.create_record({"partition_key":workflow_name, "sort_key":str(True)+"~"+"template"+"~"+str(new_version), "wf_config_uuid":str(uuid.uuid4()), "wf_uuid":wf_uuid, "param_name":"template", "param_value":template, "version":new_version, "is_active":True,"created_date":datetime.now().isoformat(),"updated_date":datetime.now().isoformat(),"transaction_id":transaction_id}, conn)

    if engine is not None:
        wf_config_uuid = wfc.create_record({"partition_key":workflow_name, "sort_key":str(True)+"~"+"engine_type"+"~"+str(new_version), "wf_config_uuid":str(uuid.uuid4()), "wf_uuid":wf_uuid, "param_name":"engine_type", "param_value":engine, "version":new_version, "is_active":True,"created_date":datetime.now().isoformat(),"updated_date":datetime.now().isoformat(),"transaction_id":transaction_id}, conn)

    if sys_config is not None:
        wf_config_uuid = wfc.create_record({"partition_key":workflow_name, "sort_key":str(True)+"~wf_config~sys_config"+"~"+str(new_version), "wf_config_uuid":str(uuid.uuid4()), "wf_uuid":wf_uuid, "param_name":"system_config_table", "param_value":sys_config, "version":new_version, "is_active":True,"created_date":datetime.now().isoformat(),"updated_date":datetime.now().isoformat(),"transaction_id":transaction_id}, conn)
    
    #Add dataset
    jobs = payload["data"]["jobs"]

    # workflow config
    jb = JobDAO()
    #Read old records
    record_job = jb.read_record(record = {"key":{"partition_key":workflow_name}}, connector = conn)

    #Reset old version to 0
    old_version_job = 0
    #Inactivate old records
    if len(record_job)>0:
        for itm in record_job:
            old_version_job = itm["version"] if itm["version"]>old_version_job else old_version_job

            if itm["is_active"] is True:
                jb.update_record({"key":{"partition_key":workflow_name, "sort_key":itm["sort_key"]}, "update_item":[{"update_col": "is_active", "update_val":False}]}, conn)

    #increment version by 1
    new_version_job = old_version_job + 1
    
    #dataset DAO object
    dso = DataSetDAO()

    #read old dataset record for workflow
    record_dataset = dso.read_record(record = {"key":{"partition_key":workflow_name}}, connector = conn)

    #Reset old version to 0
    old_version_dataset = 0
    #Inactivate old records
    if len(record_dataset)>0:
        for itm in record_dataset:
            old_version_dataset = itm["version"] if itm["version"]>old_version_dataset else old_version_dataset
            if itm["is_active"] is True:
                dso.update_record({"key":{"partition_key":workflow_name, "sort_key":itm["sort_key"]}, "update_item":[{"update_col": "is_active", "update_val":False}]}, conn)

    #increment version by 1
    new_version_dataset = old_version_dataset + 1
    
    #Job config DAO
    jobc = JobConfigDAO()
    #read old dataset record for workflow
    record_jobc = jobc.read_record(record = {"key":{"partition_key":workflow_name}}, connector = conn)

    #Reset old version to 0
    old_version_jobc = 0
    #Inactivate old records
    if len(record_jobc)>0:
        for itm in record_jobc:
            old_version_jobc = itm["version"] if itm["version"]>old_version_jobc else old_version_jobc

            if itm["is_active"] is True:
                jobc.update_record({"key":{"partition_key":workflow_name, "sort_key":itm["sort_key"]}, "update_item":[{"update_col": "is_active", "update_val":False}]}, conn)

    #increment version by 1
    new_version_jobc = old_version_jobc + 1

    #Job config DAO
    jobp = JobParamDAO()
    #read old dataset record for workflow
    record_jobp = jobp.read_record(record = {"key":{"partition_key":workflow_name}}, connector = conn)

    #Reset old version to 0
    old_version_jobp = 0
    #Inactivate old records
    if len(record_jobp)>0:
        for itm in record_jobp:
            old_version_jobp = itm["version"] if itm["version"]>old_version_jobp else old_version_jobp

            if itm["is_active"] is True:
                jobp.update_record({"key":{"partition_key":workflow_name, "sort_key":itm["sort_key"]}, "update_item":[{"update_col": "is_active", "update_val":False}]}, conn)

    #increment version by 1
    new_version_jobp = old_version_jobp + 1

    print("Old records inactivated")
    #####################################
    #Loop for all jobs in the config file
    #####################################
    print("Proceeding with new record update")

    for job in list(jobs):
        job_uuid = jb.create_record({"partition_key":workflow_name, "sort_key":str(True)+"~"+job["job_name"]+str(new_version_job), "job_uuid": str(uuid.uuid4()), "job_name":job["job_name"], "wf_uuid": wf_uuid, "job_desc": job.get("job_desc",None), "job_engine_type": job.get("job_engine_type",None), "job_template_name": job.get("job_template_name",None), "job_priority": job.get("job_priority",1), "version":new_version_job, "is_active":True,"created_date":datetime.now().isoformat(),"updated_date":datetime.now().isoformat(),"transaction_id":transaction_id}, conn)

        d = dso.create_record({"partition_key": workflow_name, "sort_key": job_uuid+"~"+job["job_params"]["source_db"]+"."+job["job_params"]["source_table"], "dataset_uuid":str(uuid.uuid4()),"dataset_name":job["job_params"]["source_db"]+"."+job["job_params"]["source_table"],"job_uuid":job_uuid,"is_active":True,"created_date":datetime.now().isoformat(),"updated_date":datetime.now().isoformat(),"version":new_version_dataset,"transaction_id":transaction_id},  conn)
        d = dso.create_record({"partition_key": workflow_name, "sort_key": job_uuid+"~"+job["job_params"]["target_db"]+"."+job["job_params"]["target_table"], "dataset_uuid":str(uuid.uuid4()),"dataset_name":job["job_params"]["target_db"]+"."+job["job_params"]["target_table"],"job_uuid":job_uuid,"is_active":True,"created_date":datetime.now().isoformat(),"updated_date":datetime.now().isoformat(),"version":new_version_dataset,"transaction_id":transaction_id},  conn)

        if job.get("job_config_override",None) is not None:
            for job_c in job["job_config_override"].keys():
                jc = jobc.create_record({"partition_key":workflow_name, "sort_key":str(True)+"~"+job_c+"~"+job_uuid+"~"+str(new_version_jobc), "param_name":job_c, "param_value":job["job_config_override"][job_c], "version":new_version_jobc, "is_active":True,"created_date":datetime.now().isoformat(),"updated_date":datetime.now().isoformat(),"job_uuid":job_uuid, "job_config_id":str(uuid.uuid4()), "transaction_id":transaction_id}, conn)

        if job.get("job_params",None) is not None:
            for job_p in job["job_params"].keys():
                jp = jobp.create_record({"partition_key":workflow_name, "sort_key":str(True)+"~"+job_p+"~"+job_uuid+"~"+str(new_version_jobp), "param_name":job_p, "param_value":job["job_params"][job_p], "version":new_version_jobp, "is_active":True,"created_date":datetime.now().isoformat(),"updated_date":datetime.now().isoformat(),"job_uuid":job_uuid, "job_param_uuid":str(uuid.uuid4()), "job_config_id":str(uuid.uuid4()), "transaction_id":transaction_id}, conn)

def trigger(payload: dict):
    #Create a command factory and start execution of onboarding using handler onboard function
    cf = CommandFactory(Command.ONBOARD).handler(onboard)
    response = cf.execute(data=payload)

