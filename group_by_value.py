from collections import defaultdict
import boto3
import json


def extract_body(response):
    payload = json.loads(response['Body'].read())
    return payload


def key_func(k):
    return k['company']


def lambda_handler(event, context):
    dynamo_client = boto3.resource(service_name='dynamodb', region_name="eu-central-1")
    key = event
    table = dynamo_client.Table('intermediate1')
    data = table.get_item(Key={'id': key})['Item']['value']
    data = json.loads(data)

    result = defaultdict(list)

    for key, val in sorted(data.items()):
        result[val].append(key)

    return {
        'statusCode': 200,
        'body': result
    }
