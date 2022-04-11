from __future__ import    annotations
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from BaseDAO import BaseDAO
from typing import TypedDict
import boto3
from core.connection import *
from types import *
from datetime import datetime


class WorkFlowDAO(BaseDAO):

    def create_record(self, record: TypedDict):
        return WorkFlow(record)
    def read_record(self, key: str)-> DataSet:
        return WorkFlow({"wf_uuid":"demo","wf_name":key,"job_schedule_uuid":"demo","is_active":True,"created_date":datetime.now(),"updated_date":datetime.now(),"version":1})

