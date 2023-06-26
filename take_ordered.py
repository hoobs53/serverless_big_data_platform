import boto3
import json


def lambda_handler(event, _):
    dynamo_client = boto3.resource(service_name='dynamodb', region_name="eu-central-1")
    key = event['id']
    num = int(event['no_of_elements'])
    ascending = event['ascending']
    table = dynamo_client.Table('intermediate1')
    data = table.get_item(Key={'id': key})['Item']['value']
    data = json.loads(data)

    if ascending == "false":
        list.sort(data, reverse=True)
    else:
        list.sort(data)

    if num < len(data):
        result_set = data[:num + 1]
    else:
        result_set = data

    return {
        'statusCode': 200,
        'body': json.dumps(result_set)
    }
