from __future__ import annotations
from enum import Enum
import boto3
import os 
from abc import ABC, abstractmethod
from typing import Optional, Any
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))


class Connection(ABC):

    @abstractmethod
    def connect(self)->Optional[Any]:
        pass
    @abstractmethod
    def option(self, key_name: str, key_value: str)->Optional[Any]:
        pass

    @abstractmethod
    def query(self, table: str, record: Dict):
        pass
    
    @abstractmethod
    def add(self, table: str, record: Dict):
        pass

    @abstractmethod
    def update(self, table: str, record: Dict):
        pass

    @abstractmethod
    def delete(self, table: str, record: Dict):
        pass
class DynamoDBConnection(Connection):
    '''
        This class is sub classed from connection class to create dynamodb connection
    '''
    
    def __init__(self):
        #Read region from environment variable during init call. If variable does not exist, use us-east-1 as default
        try:
            self.region = os.environ["AWS_REGION"]
            self.profile = os.environ.get("AWS_PROFILE",None)
            self.dynamodb = ""
        except Exception as e:
            self.region = 'us-east-1'
            self.profile = None

    def connect(self)->DynamoDBConnection:
        try:
            if self.profile is not None:
                session = boto3.Session(profile_name=self.profile)
            else:
                session = boto3.Session()
            self.dynamodb = session.resource("dynamodb", region_name = self.region)
   
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

    def query(self, table: str, record: Dict):
        tbl = self.dynamodb.Table(table)
        filter_exp = ""
        for key in record["key"].keys():
            if filter_exp == "":
                filter_exp = Key(key).eq(record["key"][key])
            else:
                filter_exp = filter_exp & Key(key).eq(record["key"][key])    
        
        resp = tbl.query(KeyConditionExpression=filter_exp)   
        data = resp["Items"]
        while 'LastEvaluatedKey' in resp:
            resp = tbl.query(KeyConditionExpression=filter_exp, ExclusiveStartKey=response['LastEvaluatedKey']) 
            data.extend(response['Items'])
        return data

    def add(self, table: str, record: BaseDAO):
        tbl = self.dynamodb.Table(table)
        tbl.put_item(Item=record)

    def update(self, table: str, record: Dict):
        tbl = self.dynamodb.Table(table)
        update_exp = "set "
        update_attr = {}
        for update_record in record["update_item"]:
            if update_exp != "set ":
                update_exp = update_exp+ " and "
            update_exp = update_exp + update_record["update_col"] +" = "+ ":"+update_record["update_col"]
            update_attr[":"+update_record["update_col"]] = update_record["update_val"]
        response = tbl.update_item(
            Key=record["key"],
            UpdateExpression=update_exp,
            ExpressionAttributeValues = update_attr,
            ReturnValues="UPDATED_NEW"
        )

    def delete(self, table: str, key: str):
        pass
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