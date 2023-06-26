import boto3
import json


def lambda_handler(event, _):
    dynamo_client = boto3.resource(service_name='dynamodb', region_name="eu-central-1")
    event_json = json.loads(event)
    key = event_json['id']
    num = event_json['no_of_elements']
    table = dynamo_client.Table('intermediate1')
    dynamo_data = table.get_item(Key={'id': key})['Item']['value']
    data = json.loads(dynamo_data)
    
    if num < len(data):
        result_set = data[:num + 1]
    else:
        result_set = data

    return {
        'statusCode': 200,
        'body': json.dumps(result_set)
    }
