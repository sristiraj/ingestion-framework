from __future__ import    annotations
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import enum
import boto3
import os 
from abc import ABC, abstractmethod
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

class JobEngineType(enum.Enum):
    GLUESPARKJOB = 'GLUESPARKJOB'
    GLUEPYTHONSHELL = 'GLUEPYTHONSHELL'
    AWSLAMBDA = 'AWSLAMBDA'

    @classmethod
    def has_value(cls, value):
        return value.name in cls._value2member_map_.keys()

    @classmethod
    def get_instance(cls, val):
        if val == "GLUESPARKJOB":
            return cls.GLUESPARKJOB
        elif val == "GLUEPYTHONSHELL":
            return cls.GLUEPYTHONSHELL
        elif val == "AWSLAMBDA":
            return cls.AWSLAMBDA
        else:
            return None            

class Engine(ABC):
    def __init__(self):
        self._name = "engine configuration"

    @abstractmethod
    def start(self, job_arn: str, arguments: dict):
        pass

    @abstractmethod
    def monitor(self, job_arn: str, run_id: str):
        pass

    @abstractmethod
    def deploy(self, workflow_name: str):
        pass


class GlueSparkJobEngine(Engine):
    def start(self, job_arn: str, arguments: dict):
        session = boto3.session.Session()
        glue_client = session.client('glue')

        try:
            job_run_id = glue_client.start_job_run(JobName=job_arn, Arguments=arguments)
        except ClientError as e:
            raise Exception( "boto3 client error in run_glue_job: " + e.__str__())
        except Exception as e:
            raise Exception( "Unexpected error in run_glue_job: " + e.__str__())


        return job_run_id["JobRunId"]
    def monitor(self, job_arn: str, run_id: str):
        session = boto3.session.Session()
        glue_client = session.client('glue')

        try:
            status_detail = glue_client.get_job_run(JobName=job_arn, RunId = run_id)

            status = status_detail.get("JobRun").get("JobRunState")
            if status.lower() == "running":
                return "running"
            elif status.lower() == "completed":
                return "completed"
            elif status.lower() == "error" or status.lower() == "failed":
                return "error"    
        except ClientError as e:
            raise Exception( "boto3 client error in run_glue_job: " + e.__str__())
        except Exception as e:
            raise Exception( "Unexpected error in run_glue_job: " + e.__str__())

    def deploy(self, workflow_name: str):
        pass

class EngineFactory(object):
    def __init__(self, engine_type: str):
        if not JobEngineType.has_value(engine_type):
            raise ValueError(f"{engine_type} is not a member of EngineType class.")
        if engine_type == JobEngineType.GLUESPARKJOB:
            self.engine_type = GlueSparkJobEngine()
    
    def get_engine(self):
        return self.engine_type

if __name__=="__main__":
    print(JobEngineType.get_instance("GLUESPARKJOB"))