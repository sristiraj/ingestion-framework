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
import json
import boto3



logger = logging.getLogger(__file__)
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:793340215062:ingestionFramework"
#SES_EMAIL = ""


def notify(payload: dict):
    conn = ConnectionFactory(ConnectionType.DYNAMODB).get_connection().connect()
    
    print("Received payload {}".format(json.dumps(payload)))
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
        if run.get("wf_run_event_id","") == wf_run_event_id:
            job_det["wf_run_event_id"] = wf_run_event_id
            job_det["wf_name"] = run["partition_key"]
            job_det["run_id"] = run["run_id"]
            job_det["job_name"] = run["job_name"]
            job_det["job_status"] = run["job_status"]
            job_loop.append(job_det)
    
    
    message = "wf_run_event_id | wf_name | run_id | job_name | job_status \n"
    for job in job_loop:
        message = message + job["wf_run_event_id"]+" | "+job["wf_name"]+" | "+job["run_id"]+" | "+job["job_name"]+" | "+job["job_status"]+" \n"
    client = boto3.client('sns')
    client.publish(TopicArn=SNS_TOPIC_ARN, Message= message, Subject="Automated Data Ingestion Notification: Do not reply")

#     SES delivery code (In case of SES usage)
#     ses_client = boto3.client("ses", region_name="eu-east-1")
#     CHARSET = "UTF-8"

#     response = ses_client.send_email(
#         Destination={
#             "ToAddresses": [
#                 SES_EMAIL,
#             ],
#         },
#         Message={
#             "Body": {
#                 "Text": {
#                     "Charset": CHARSET,
#                     "Data": message,
#                 }
#             },
#             "Subject": {
#                 "Charset": CHARSET,
#                 "Data": "Automated Data Ingestion Notification: Do not reply",
#             },
#         },
#         Source=SES_EMAIL,
#     )
    return job_loop
    
def trigger(payload: dict):
    #Create a command factory and start execution of onboarding using handler onboard function
    cf = CommandFactory(Command.NOTIFY).handler(notify)
    response = cf.execute(data=payload)
    return response

