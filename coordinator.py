import json
from time import time

import boto3
import itertools
from concurrent.futures import ThreadPoolExecutor
from collections import deque

lambda_client = boto3.client('lambda', region_name="eu-central-1")
dynamo_client = boto3.resource(service_name='dynamodb', region_name="eu-central-1")
table = dynamo_client.Table('intermediate1')
s3_client = boto3.client('s3', region_name="eu-central-1")

stats = {}
num_of_batches = 3


def init_logs(lambdas_to_run):
    global stats
    stats = {"splits_in_total": 0, "lambdas_executed": [], "merges_in_total": 0, "total_time": 0,
             "lambda_execution_times": {}}
    for lambda_to_run in lambdas_to_run:
        stats["lambda_execution_times"][lambda_to_run["name"]] = 0


def get_from_s3(key):
    data = s3_client.get_object(Bucket='intermediate1-sbg-bucket', Key=str(key))
    data = extract_body(data)
    return data


def load_input(input_file):
    data = get_from_s3(input_file)
    return data


def extract_payload(response):
    payload = json.loads(response['Payload'].read())
    print(payload)
    return payload['body']


def extract_body(response):
    payload = json.loads(response['Body'].read())
    return payload


def handle_requests(lambdas, data):
    i = 0
    function_to_run = "None"
    for function_name in lambdas:
        st = time()
        function_to_run = function_name['name']
        if not should_batch(function_to_run):
            break
        print("Invoking function '%s'..." % function_to_run)
        stats["lambdas_executed"].append(function_to_run)
        if 'func' in function_name:
            payload = {'id': data, 'func': function_name['func']}
        else:
            payload = {'id': data}
        response = lambda_client.invoke(
            FunctionName=function_to_run,
            Payload=json.dumps(payload),
            LogType='Tail')
        data = extract_payload(response)
        i += 1
        et = time()
        stats["lambda_execution_times"][function_to_run] += et - st
        if i == len(lambdas):
            function_to_run = "None"
    return data, function_to_run


def handle_one_request(lambdas, data):
    st = time()
    function_to_run = lambdas[0]['name']
    stats["lambdas_executed"].append(function_to_run)
    print("Invoking function '%s'..." % function_to_run)
    payload = {'id': data}
    if 'func' in lambdas[0]:
        payload['func'] = lambdas[0]['func']
    if 'algorithm' in lambdas[0]:
        payload['algorithm'] = lambdas[0]['algorithm']
    if 'ascending' in lambdas[0]:
        payload['ascending'] = lambdas[0]['ascending']
    if 'no_of_elements' in lambdas[0]:
        payload['no_of_elements'] = lambdas[0]['no_of_elements']
    response = lambda_client.invoke(
        FunctionName=function_to_run,
        Payload=json.dumps(payload),
        LogType='Tail')
    data = extract_payload(response)
    et = time()
    stats["lambda_execution_times"][function_to_run] += et - st
    if len(lambdas) == 1:
        function_to_run = "None"
    else:
        function_to_run = lambdas[1]['name']
    return data, function_to_run


def handle_sort_request(lambda_data, data):
    futs = []
    data_batches = split_list(data, num_of_batches)
    i = 1
    with ThreadPoolExecutor(max_workers=num_of_batches) as executor:
        for batch in data_batches:
            file_key = i
            table.put_item(Item={'id': file_key, 'value': json.dumps(batch)})
            futs.append(
                executor.submit(handle_one_request,
                                lambdas=[lambda_data],
                                data=file_key
                                )
            )
            i += 1
        data = [fut.result() for fut in futs]
        data = merge_data(data)
        table.put_item(Item={'id': 50, 'value': json.dumps(data)})
        data = handle_one_request(lambdas=[{'name': 'sort', 'algorithm': lambda_data['algorithm']}], data=50)[0]
        data = get_from_dynamo(data)
        return data


def get_from_dynamo(key):
    data = json.loads(table.get_item(Key={'id': key})['Item']['value'])
    return data


def merge_data(data):
    stats["merges_in_total"] += 1
    dynamo_data = []
    for d in data:
        print(d)
        d_dynamo = json.loads(table.get_item(Key={'id': d[0]})['Item']['value'])
        if not isinstance(d_dynamo, list):
            return [d_dynamo]
        for d2 in d_dynamo:
            dynamo_data.append(d2)
    result = []
    for d in dynamo_data:
        result.append(d)
    return result


def split_list(alist, wanted_parts=3):
    if not isinstance(alist, list):
        return [alist]
    stats["num_of_batches"] = wanted_parts
    stats["splits_in_total"] += 1
    length = len(alist)
    return [alist[i * length // wanted_parts: (i + 1) * length // wanted_parts]
            for i in range(wanted_parts)]


def should_batch(lambda_name):
    return lambda_name not in ['first', 'take', 'take_ordered', 'count', 'reduce', 'group_by_key', 'group_by_value',
                               'reduce_by_key',
                               'union', 'sort', 'intersection', 'distinct']


def lambda_handler(event, _):
    global num_of_batches
    st = time()
    lambdas_to_run = event['lambdas']
    init_logs(lambdas_to_run)
    lambdas_left = deque(lambdas_to_run)
    data = load_input(event['input'])
    # data = event['data']
    if 'num_of_batches' in event:
        num_of_batches = event['num_of_batches']

    iterations = 0

    while iterations < (len(lambdas_to_run) + 1):
        print(lambdas_left)
        if lambdas_left[0]['name'] == 'sort':
            data = handle_sort_request(lambdas_left[0], data)
            lambdas_left.popleft()
            if len(lambdas_left) == 0:
                break
        elif should_batch(lambdas_left[0]['name']):
            futs = []
            data_batches = split_list(data, num_of_batches)
            i = 1
            with ThreadPoolExecutor(max_workers=num_of_batches) as executor:
                for batch in data_batches:
                    file_key = i
                    table.put_item(Item={'id': file_key, 'value': json.dumps(batch)})
                    futs.append(
                        executor.submit(handle_requests,
                                        lambdas=lambdas_left,
                                        data=file_key
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
                    next_func = data[0][1]
                    lambdas_left = deque(itertools.dropwhile(lambda x: x['name'] != next_func, lambdas_left))
                    data = merge_data(data)
        else:
            if lambdas_left[0]['name'] == 'intersection' or lambdas_left[0]['name'] == 'union':
                table.put_item(Item={'id': 51, 'value': json.dumps(data[0])})
                table.put_item(Item={'id': 52, 'value': json.dumps(data[1])})
                data = handle_one_request(
                    lambdas=lambdas_left,
                    data=[51, 52]
                )
            else:
                table.put_item(Item={'id': 50, 'value': json.dumps(data)})
                data = handle_one_request(
                    lambdas=lambdas_left,
                    data=50
                )
            next_func = data[1]
            data = data[0]
            if next_func == "None":
                break
            lambdas_left.popleft()
        iterations += 1

    et = time()
    stats["total_time"] = et - st
    result = {"data": data, "stats": stats}
    s3_client.put_object(Body=json.dumps(result), Key=event['output'], Bucket='intermediate1-sbg-bucket')
    return {
        'statusCode': 200,
        'body': result
    }
