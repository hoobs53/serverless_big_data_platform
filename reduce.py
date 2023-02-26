import boto3
import json
from reduce_func import do_reduce
from functools import reduce


def extract_body(response):
    payload = json.loads(response['Body'].read())
    return payload


def lambda_handler(event, context):
    dynamo_client = boto3.resource(service_name='dynamodb', region_name="eu-central-1")
    key = event
    table = dynamo_client.Table('intermediate1')
    data = table.get_item(Key={'id': key})['Item']['value']

    result = [reduce(do_reduce, data)]

    table.put_item(Item={'id': key, 'value': result, 'type': 'int set'})
    return {
        'statusCode': 200,
        'body': result
    }
