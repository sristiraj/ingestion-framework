from __future__ import annotations
from enum import Enum
import boto3
import os 
from abc import ABC, abstractmethod
from typing import Optional, Any
from datetime import datetime
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from core.dao import BaseDAO

class Connection(ABC):

    @abstractmethod
    def connect(self)->Optional[Any]:
        pass
    @abstractmethod
    def option(self, key_name: str, key_value: str)->Optional[Any]:
        pass

    @abstractmethod
    def query(self, connection: Connection, table: str, key_name: str, key_value: str):
        pass
    
    @abstractmethod
    def add(self, connection: Connection, table: str, record: BaseDAO):
        pass

class DynamoDBConnection(Connection):
    '''
        This class is sub classed from connection class to create dynamodb connection
    '''
    
    def __init__(self):
        #Read region from environment variable during init call. If variable does not exist, use us-east-1 as default
        try:
            self.region = os.environ["AWS_REGION"]
            self.dynamodb = ""
        except Exception as e:
            self.region = 'us-east-1'

    def connect(self)->DynamoDBConnection:
        try:
            self.dynamodb = boto3.resource("dynamodb", region_name = self.region)
            return self
        except:
            raise Exception("Error Connecting to dynamodb")
        
    def option(self, key_name: str, key_value: str) -> DynamoDBConnection:
        if  key_name == "region":
            self.AWS_REGION = key_value  
        return self  

    def scan(self, table):
        tbl = self.dynamodb.Table(table)
        resp = tbl.scan()
        data = resp["Items"]
        while 'LastEvaluatedKey' in resp:
            resp = tbl.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])
        return data

    def query(self, table, key_name, key_val):
        tbl = self.dynamodb.Table(table)
        resp = tbl.query(KeyConditionExpression=Key(key_name).eq(key_val))   
        data = resp["Items"]
        while 'LastEvaluatedKey' in resp:
            resp = tbl.query(KeyConditionExpression=Key(key_name).eq(key_val), ExclusiveStartKey=response['LastEvaluatedKey']) 
            data.extend(response['Items'])
        return data

    def add(self, table: str, record: BaseDAO):
        tbl = self.dynamodb.Table(table)
        tbl.put_item(Item=record)

class ConnectionType(Enum):
    DYNAMODB = 'DYNAMODB'

    @classmethod
    def has_value(cls, value):
        return value.name in cls._value2member_map_.keys()


class ConnectionFactory(object):


    def __init__(self, conn_type: ConnectionType):
        if not ConnectionType.has_value(conn_type):
            raise ValueError(f"{conn_type} is not a member of ConnectionType class.")
        if conn_type == ConnectionType.DYNAMODB:
            self.connection = DynamoDBConnection()

    def get_connection(self):
        return self.connection


if __name__ == "__main__":
    conn = ConnectionFactory(ConnectionType.DYNAMODB).get_connection().connect()
    conn.add("dataset",{"dataset_uuid":"demo","dataset_name":"key","job_uuid":"demo","is_active":True,"created_date":datetime.now().isoformat(),"updated_date":datetime.now().isoformat(),"version":1})