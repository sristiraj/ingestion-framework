import boto3
import json
import typing

def invokeLambdaFunction(*, functionName:str=None, payload:typing.Mapping[str, str]=None):
    if  functionName == None:
        raise Exception('ERROR: functionName parameter cannot be NULL')
    payloadStr = json.dumps(payload)
    payloadBytesArr = bytes(payloadStr, encoding='utf8')
    client = boto3.client('lambda')
    response = client.invoke(
        FunctionName=functionName,
        InvocationType="RequestResponse",
        Payload=payloadBytesArr,
        LogType="Tail"
    )
    return response
payloadObj = {"wf_name":"customer_load_wf_start"}
response = invokeLambdaFunction(functionName='ingestion_framework_start',  payload=payloadObj)
print(f'response:{response}')

# arn:aws:lambda:us-east-1:793340215062:function:ingestion_framework_start