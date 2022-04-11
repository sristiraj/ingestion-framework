import pytest
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from ingestion_framework.onboard import trigger
import json


def test_onboard():
    dirrectory = os.path.dirname(__file__)
    file = os.path.dirname(__file__)+"\\resources\\input.json"
    with open(file) as f:
        js_data = json.load(f)
    print(type(js_data))    
    trigger(js_data)

if __name__ == "__main__":
    test_onboard()    