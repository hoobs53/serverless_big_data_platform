import json

import boto3


def extract_body(response):
    payload = json.loads(response['Body'].read())
    return payload


def lambda_handler(event, context):
    dynamo_client = boto3.resource(service_name='dynamodb', region_name="eu-central-1")
    keys = event['id']
    table = dynamo_client.Table('intermediate1')

    result = []
    s3_data = []
    for k in keys.items():
        dynamo_data = table.get_item(Key={'id': k})['Item']['value']
        data = json.loads(dynamo_data)
        for d in data:
            s3_data.append(d)
    for d in s3_data:
        result.append(d)

    return {
        'statusCode': 200,
        'body': result
    }
