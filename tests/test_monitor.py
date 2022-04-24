import pytest
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from ingestion_framework.monitor import trigger
import json


def test_trigger():
    workflow_name = "customer_load_wf_start"
    monitor_command = {"wf_name":workflow_name}
    trigger(monitor_command)

if __name__ == "__main__":
    test_trigger()    