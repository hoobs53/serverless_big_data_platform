import boto3
import json
from take_ordered_func import comparator_func


def extract_body(response):
    payload = json.loads(response['Body'].read())
    return payload


def lambda_handler(event, context):
    dynamo_client = boto3.resource(service_name='dynamodb', region_name="eu-central-1")
    event_json = json.loads(event)
    key = event_json['key']
    num = event_json['no_of_elements']
    table = dynamo_client.Table('intermediate1')
    data = table.get_item(Key={'id': key})['Item']['value']

    list.sort(data, key=comparator_func)

    if num < len(data):
        result_set = data[:num + 1]
    else:
        result_set = data

    return {
        'statusCode': 200,
        'body': json.dumps(result_set)
    }