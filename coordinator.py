import json
import boto3
import itertools
from concurrent.futures import ThreadPoolExecutor
from collections import deque

lambda_client = boto3.client('lambda', region_name="eu-central-1")
dynamo_client  =  boto3.resource(service_name = 'dynamodb', region_name="eu-central-1")
table = dynamo_client.Table('intermediate1')

def extract_payload(response):
    payload = json.loads(response['Payload'].read())
    return payload['body']

def extract_body(response):
    payload = json.loads(response['Body'].read())
    return payload

def handle_requests(lambdas, data):
    function_to_run = lambdas[0]
    i = 0
    for function_name in lambdas:
        function_to_run = function_name
        if function_name in ['first', 'take', 'count', 'reduce', 'group_by_key', 'group_by_value', 'reduce_by_key', 'union', 'first2']:
            break
        print("Invoking function '%s'..." % function_name)
        response = lambda_client.invoke(
                FunctionName=function_name,
                Payload=json.dumps(data),
                LogType='Tail')
        data = extract_payload(response)
        i += 1
        if i == len(lambdas):
            function_to_run = "None"
    return (data, function_to_run)

def handle_one_request(lambdas, data):
    function_to_run = lambdas[0]
    print("Invoking function '%s'..." % function_to_run)
    response = lambda_client.invoke(
            FunctionName=function_to_run,
            Payload=json.dumps(data),
            LogType='Tail')
    data = extract_payload(response)
    if len(lambdas)==1:
        function_to_run = "None"
    else:
        function_to_run = lambdas[1]
    return (data, function_to_run)

def get_from_dynamo(key):
    data = table.get_item(Key = {'id': key})
    return extract_body(data)

def merge_data(data):
    dynamo_data = []
    for d in data:
        for d2 in table.get_item(Key = {'id': d[0]})['Item']['value']:
            dynamo_data.append(d2)
    result = []
    for d in dynamo_data:
        result.append(d)
    return result

def split_list(alist, wanted_parts=3):
    length = len(alist)
    return [ alist[i*length // wanted_parts: (i+1)*length // wanted_parts] 
             for i in range(wanted_parts) ]

def should_batch(lambda_name):
    return lambda_name not in ['first', 'take', 'count', 'reduce', 'group_by_key', 'group_by_value', 'reduce_by_key', 'union', 'first2']

def lambda_handler(event, context):
    lambdas_to_run=event['lambdas']
    lambdas_left=deque(lambdas_to_run)
    data=event['data']
    data_len = len(data)
    security_grzybek = 0
    
    while True:
        print(lambdas_left)
        if should_batch(lambdas_left[0]):
            futs = []
            data_batches = split_list(data)
            i = 1
            with ThreadPoolExecutor(max_workers=3) as executor:
                for batch in data_batches:
                    bucket_key = "intermediate1"
                    file_key = i
                    table.put_item(Item = {'id':file_key, 'value': batch, 'type': 'int set'})
                    futs.append(
                        executor.submit(handle_requests,
                            lambdas = lambdas_left,
                            data = file_key
                        )
                    )
                    i += 1
                data = [fut.result() for fut in futs]
                if data[0][1] == "None":
                    print("Next func: " + data[0][1])
                    data = merge_data(data)
                    break
                else:
                    print("Else next func: " + data[0][1])
                    next_func=data[0][1]
                    lambdas_left=deque(itertools.dropwhile(lambda x: x != next_func, lambdas_left))
                    data = merge_data(data)
        else:
            bucket_key="intermediate1"
            file_key="input-batch1.json"
            print("one: " + str(data))
            table.put_item(Item = {'id':50, 'value': data, 'type': 'int set'})
            data = handle_one_request(
                lambdas = lambdas_left,
                data = 50
                )
            next_func=data[1]
            data = data[0]
            if next_func == "None":
                break
            lambdas_left.popleft()
        security_grzybek += 1
        if security_grzybek >= 10:
            print("security grzybek")
            break
    return {
        'statusCode': 200,
        'body': data
    }