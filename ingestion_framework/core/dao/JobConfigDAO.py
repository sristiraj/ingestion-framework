from __future__ import    annotations
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from BaseDAO import BaseDAO
from typing import TypedDict
import boto3
from core.connection import *
from types import *
from datetime import datetime
from core.dao.types import *


class JobConfigDAO(BaseDAO):
    def __init__(self):
        self.__name__ = "JobConfigDAO"
        self.datastore = "job_config"
    
    def create_record(self, record: TypedDict, connector: Conn):
        data =  JobConfig(record)
        connector.add(self.datastore, data)
        return record["job_config_id"]

    def read_record(self, record: Dict, connector: Connection)-> DataSet:
        return connector.query(self.datastore, record)

    def update_record(self, record: Dict, connector: Connection):
        return connector.update(self.datastore, record)
        

if __name__ == "__main__":
    JobConfigDAO()