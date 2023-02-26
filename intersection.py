import boto3
import json
from map_func import mapping_function as func

def extract_body(response):
    payload = json.loads(response['Body'].read())
    return payload

def lambda_handler(event, context):
    dynamo_client  =  boto3.resource(service_name = 'dynamodb', region_name="eu-central-1")
    keys = json.loads(event)
    table = dynamo_client.Table('intermediate1')
    result = []
    dynamo_data = []
    for k in keys.items():
        data = table.get_item(Key = {'id': k})['Item']['value']
        dynamo_data.append(data)
    result = list(set.intersection(*map(set,dynamo_data)))
    
    return {
        'statusCode': 200,
        'body': result
    }