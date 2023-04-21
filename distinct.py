import boto3
import json


def lambda_handler(event, context):
    dynamo_client = boto3.resource(service_name='dynamodb', region_name="eu-central-1")
    key = event['id']
    table = dynamo_client.Table('intermediate1')
    data = table.get_item(Key={'id': key})['Item']['value']
    data = json.loads(data)
    result_set = []

    for i in data:
        if not i in result_set:
            result_set.append(i)

    return {
        'statusCode': 200,
        'body': result_set
    }
