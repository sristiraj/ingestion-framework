import json
import logging 
import boto3
from ingestion_framework.notify import trigger

logger = logging.getLogger(__file__)


def lambda_handler(event, context):
    logger.info(event)
    logger.info('## INITIATED BY EVENT: ')

    print(event)
    
    
    response = trigger(event)
        
    return response
