import pytest
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from ingestion_framework.core.connection import *
import json
import uuid
from datetime import datetime


def test_create_new_record():
    conn = ConnectionFactory(ConnectionType.DYNAMODB).get_connection().connect()
    tbl = "workflow_config"

    record  = {"partition_key":"demo_wf", "sort_key":"true", "wf_config_uuid":str(uuid.uuid4()), "wf_uuid":"demo", "param_name":"test_param", "param_value":"test_val", "version":1, "is_active":True,"created_date":datetime.now().isoformat(),"updated_date":datetime.now().isoformat(),"transaction_id":"demo_trx_id"}
    conn.add("workflow_config", record)

def test_update_record():
    conn = ConnectionFactory(ConnectionType.DYNAMODB).get_connection().connect()
    tbl = "workflow_config"
    record = {"key":{"partition_key":"demo_wf", "sort_key":"true"}, "update_item":[{"update_col": "is_active", "update_val":False}]}
    conn.update(tbl, record)

def test_query_record():
    conn = ConnectionFactory(ConnectionType.DYNAMODB).get_connection().connect()
    tbl = "workflow_config"
    record = {"key":{"partition_key":"demo_wf", "sort_key":"true"}}
    print(conn.query(tbl, record))

if __name__=="__main__":
    test_create_new_record() 
    test_update_record()   
    test_query_record()