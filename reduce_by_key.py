from itertools import groupby
import boto3
import json
from functools import reduce
from operator import itemgetter


def lambda_handler(event, _):
    dynamo_client = boto3.resource(service_name='dynamodb', region_name="eu-central-1")
    event_json = event
    key = event_json['id']
    func = event_json['func']
    do_reduce = eval(func)
    table = dynamo_client.Table('intermediate1')
    dynamo_data = table.get_item(Key={'id': key})['Item']['value']
    data = json.loads(dynamo_data)

    result = ([reduce(do_reduce, group) for _, group in groupby(sorted(data), key=itemgetter(0))])

    return {
        'statusCode': 200,
        'body': result
    }