import boto3
import json


def extract_body(response):
    payload = json.loads(response['Body'].read())
    return payload


def lambda_handler(event, context):
    dynamo_client = boto3.resource(service_name='dynamodb', region_name="eu-central-1")
    key = event
    table = dynamo_client.Table('intermediate1')
    data = table.get_item(Key={'id': key})['Item']['value']
    num = 4

    result_set = data[:num + 1]

    return {
        'statusCode': 200,
        'body': json.dumps(result_set)
    }
