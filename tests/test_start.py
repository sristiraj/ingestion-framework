import pytest
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from ingestion_framework.start import trigger
import json


def test_trigger():
    workflow_name = "customer_load_wf_start"
    start_command = {"wf_name":workflow_name}
    trigger(start_command)

if __name__ == "__main__":
    test_trigger()    