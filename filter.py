import boto3
import json

def lambda_handler(event, context):
    dynamo_client = boto3.resource(service_name='dynamodb', region_name="eu-central-1")
    key = event['id']
    func = event['func']
    predicate = eval(func)
    table = dynamo_client.Table('intermediate1')
    dynamo_data = table.get_item(Key={'id': key})['Item']['value']
    data = json.loads(dynamo_data)

    result_set = []

    for i in data:
        if predicate(i):
            result_set.append(i)

    table.put_item(Item={'id': key, 'value': json.dumps(result_set), 'type': 'int set'})
    return {
        'statusCode': 200,
        'body': key
    }
