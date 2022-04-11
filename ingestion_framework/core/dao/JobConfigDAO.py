from __future__ import    annotations
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from BaseDAO import BaseDAO
from typing import TypedDict
import boto3
from core.connection import *
from types import *
from datetime import datetime


class JobConfigDAO(BaseDAO):

    def create_record(self, record: TypedDict):
        return JobConfig(record)
    def read_record(self, key: str)-> DataSet:
        return Job({"param_name":"demo","param_value":key, "is_active":True, "version":1, "created_date":datetime.now(),"updated_date":datetime.now(),"job_uuid":"demo"})

if __name__ == "__main__":
    JobConfigDAO()