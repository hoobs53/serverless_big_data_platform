from itertools import groupby
import boto3
import json


def lambda_handler(event, _):
    dynamo_client = boto3.resource(service_name='dynamodb', region_name="eu-central-1")
    event_json = event
    key = event_json['id']
    key_name = event_json['key_name']
    table = dynamo_client.Table('intermediate1')
    dynamo_data = table.get_item(Key={'id': key})['Item']['value_str']
    dynamo_data = json.loads(dynamo_data)

    data = sorted(dynamo_data, key=lambda k: k[key_name])

    result = groupby(data, lambda k: k[key_name])

    result_dict = {}

    for i, j in result:
        result_dict[i] = list(j)

    return {
        'statusCode': 200,
        'body': result_dict
    }
