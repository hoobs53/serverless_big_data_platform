import boto3
import json

def lambda_handler(event, context):
    dynamo_client  =  boto3.resource(service_name = 'dynamodb', region_name="eu-central-1")
    key = event
    table = dynamo_client.Table('intermediate1')
    data = table.get_item(Key = {'id': key})['Item']['value']
    first = data[0] if len(data) > 0 else data
    result = [first]
    return {
        'statusCode': 200,
        'body': result
    }