from itertools import groupby
import boto3
import json


def key_func(k):
    return k['company']


def lambda_handler(event, context):
    dynamo_client = boto3.resource(service_name='dynamodb', region_name="eu-central-1")
    key = event
    table = dynamo_client.Table('intermediate1')
    dynamo_data = table.get_item(Key={'id': key})['Item']['value']
    dynamo_data = json.loads(dynamo_data)
    key = dynamo_data['key']
    data = dynamo_data['data']

    data = sorted(data, key=key_func)

    result = groupby(data, key_func)

    return {
        'statusCode': 200,
        'body': result
    }
