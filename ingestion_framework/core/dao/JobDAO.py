from __future__ import    annotations
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from BaseDAO import BaseDAO
from typing import TypedDict
import boto3
from core.connection import *
from types import *
from datetime import datetime


class JobDAO(BaseDAO):

    def create_record(self, record: TypedDict):
        return Job(record)
    def read_record(self, key: str)-> DataSet:
        return Job({"job_uuid":"demo","job_name":key,"wf_uuid":"demo","job_desc":"demo","job_engine_type":JobEngineType.GLUESPARKJOB,"job_template_name":"demo", "is_active":True, "job_priority":1, "created_date":datetime.now(),"updated_date":datetime.now(),"version":1})

if __name__ == "__main__":
    JobDAO()