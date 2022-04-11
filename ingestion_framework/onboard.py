import os, sys
sys.path.append(os.path.dirname(__file__))

from util.command import CommandFactory
from util.command import Command
from core.dao import *
from core.dao.DataSetDAO import DataSetDAO
from core.connection import *
import uuid


def onboard(payload: dict):
    conn = ConnectionFactory(ConnectionType.DYNAMODB).get_connection().connect()
    jobs = payload["data"]["jobs"]
    for job in list(jobs):
        d = DataSetDAO().create_record({"dataset_uuid":uuid.uuid4(),"dataset_name":job["source_db"]+"."+["source_table"],"job_uuid":"demo","is_active":True,"created_date":datetime.now(),"updated_date":datetime.now(),"version":1})
        print(d)
    #{"dataset_uui":"demo","dataset_name":key,"job_uuid":"demo","is_active":True,"created_date":datetime.now(),"updated_date":datetime.now(),"version":1}
def trigger(payload: dict):
    
    cf = CommandFactory(Command.ONBOARD).handler(onboard)
    response = cf.execute(data=payload)

    