import boto3

from filter_func import predicate


def lambda_handler(event, context):
    dynamo_client = boto3.resource(service_name='dynamodb', region_name="eu-central-1")
    key = event
    table = dynamo_client.Table('intermediate1')
    data = table.get_item(Key={'id': key})['Item']['value']
    result_set = []

    for i in data:
        if predicate(i):
            result_set.append(i)

    table.put_item(Item={'id': key, 'value': result_set, 'type': 'int set'})
    return {
        'statusCode': 200,
        'body': event
    }
