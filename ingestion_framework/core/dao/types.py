from __future__ import annotations
from typing import TypedDict
from datetime import date, datetime
import enum


class JobEngineType(enum.Enum):
    GLUESPARKJOB = 1
    GLUEPYTHONSHELL = 2
    AWSLAMBDA = 3

class DataSet(TypedDict):
    partition_key: str
    sort_key: str
    dataset_uuid: str
    dataset_name: str
    job_uuid: str
    is_active: bool
    created_date: date
    updated_date: date
    version: int
    transaction_id: str

class WorkFlow(TypedDict):
    partition_key: str
    sort_key: str
    wf_uuid: str
    wf_name: str
    job_schedule_uuid: str
    is_active: bool
    created_date: datetime
    updated_date: datetime
    version: int
    transaction_id: str

class Job(TypedDict):
    partition_key: str
    sort_key: str
    job_uuid: str
    job_name: str
    wf_uuid: str
    job_desc: str
    job_engine_type: JobEngineType
    job_template_name: str
    is_active: bool
    job_priority: int
    created_date: datetime
    updated_date: datetime
    version: int
    transaction_id: str
class JobConfig(TypedDict):
    partition_key: str
    sort_key: str
    param_name: str
    param_value: str
    is_active: bool
    version: int
    job_config_id: str
    created_date: datetime
    updated_date: datetime
    job_uuid: str 
    transaction_id: str

class WorkFlowConfig(TypedDict):
    partition_key: str 
    sort_key: str
    wf_config_uuid: str
    wf_uuid: str
    param_name: str
    param_value: str
    is_active: bool
    created_date: datetime
    update_date: datetime
    version: int
    transaction_id: str

class Run(TypedDict):
    partition_key: str
    sort_key: str
    run_uuid: str
    run_event_wf_uuid: str
    wf_uuid: str
    job_uuid: str
    run_status: str
    run_start_time: datetime
    run_end_time: datetime
    created_date: datetime
    updated_date: datetime 

class JobSchedule(TypedDict):
    partition_key: str
    job_schedule_uuid: str
    is_active: bool
    schedule_type: str
    schedule_pattern: str
    event_source: str

class JobParam(TypedDict):
    partition_key: str 
    sort_key: str
    job_uuid: str
    param_name: str
    param_value: str
    is_active: bool
    created_date: datetime
    update_date: datetime
    version: int
    transaction_id: str    
    job_param_uuid: str