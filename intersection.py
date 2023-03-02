import json
import boto3


def extract_body(response):
    payload = json.loads(response['Body'].read())
    return payload


def lambda_handler(event, context):
    dynamo_client = boto3.resource(service_name='dynamodb', region_name="eu-central-1")
    keys = event['id']
    table = dynamo_client.Table('intermediate1')
    dynamo_data = []
    for k in keys:
        data = table.get_item(Key={'id': k})['Item']['value']
        data = json.loads(data)
        dynamo_data.append(data)
    result = list(set.intersection(*map(set, dynamo_data)))

    table.put_item(Item={'id': keys[0], 'value': json.dumps(result), 'type': 'int set'})

    return {
        'statusCode': 200,
        'body': json.dumps({'id': keys[0]})
    }
