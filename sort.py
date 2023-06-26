import boto3
import json
from time import time_ns


def insertion_sort(arr):
    if (n := len(arr)) <= 1:
        return
    for i in range(1, n):

        key = arr[i]

        # Move elements of arr[0..i-1], that are
        # greater than key, to one position ahead
        # of their current position
        j = i - 1
        while j >= 0 and key < arr[j]:
            arr[j + 1] = arr[j]
            j -= 1
        arr[j + 1] = key

    return arr


def bubble_sort(arr):
    n = len(arr)
    # optimize code, so if the array is already sorted, it doesn't need
    # to go through the entire process
    swapped = False
    # Traverse through all array elements
    for i in range(n - 1):
        # range(n) also work but outer loop will
        # repeat one time more than needed.
        # Last i elements are already in place
        for j in range(0, n - i - 1):

            # traverse the array from 0 to n-i-1
            # Swap if the element found is greater
            # than the next element
            if arr[j] > arr[j + 1]:
                swapped = True
                arr[j], arr[j + 1] = arr[j + 1], arr[j]

        if not swapped:
            # if we haven't needed to make a single swap, we
            # can just exit the main loop.
            return arr
    return arr


def partition(array, low, high):
    # choose the rightmost element as pivot
    pivot = array[high]

    # pointer for greater element
    i = low - 1

    # traverse through all elements
    # compare each element with pivot
    for j in range(low, high):
        if array[j] <= pivot:
            # If element smaller than pivot is found
            # swap it with the greater element pointed by i
            i = i + 1

            # Swapping element at i with element at j
            (array[i], array[j]) = (array[j], array[i])

    # Swap the pivot element with the greater element specified by i
    (array[i + 1], array[high]) = (array[high], array[i + 1])

    # Return the position from where partition is done
    return i + 1


def quick_sort(array, low=0, high=None):
    if high is None:
        high = len(array) - 1
    if low < high:
        # Find pivot element such that
        # element smaller than pivot are on the left
        # element greater than pivot are on the right
        pi = partition(array, low, high)

        # Recursive call on the left of pivot
        quick_sort(array, low, pi - 1)

        # Recursive call on the right of pivot
        quick_sort(array, pi + 1, high)

    return array


def lambda_handler(event, _):
    dynamo_client = boto3.resource(service_name='dynamodb', region_name="eu-central-1")
    key = event['id']
    algorithm = event['algorithm']
    table = dynamo_client.Table('intermediate1')
    dynamo_data = table.get_item(Key={'id': key})['Item']['value']
    data = json.loads(dynamo_data)
    if len(data) > 1:
        if algorithm == 'quick':
            result_set = quick_sort(data)
        elif algorithm == 'bubble':
            result_set = bubble_sort(data)
        else:
            result_set = insertion_sort(data)
    else:
        result_set = data

    table.put_item(Item={'id': key, 'value': json.dumps(result_set), 'type': 'int set'})

    return {
        'statusCode': 200,
        'body': key
    }


def run_sort(alg, data):
    st = time_ns()
    if alg == 'bubble':
        result = bubble_sort(data)
    elif alg == 'quick':
        result = quick_sort(data)
    else:
        result = insertion_sort(data)
    et = time_ns()
    return result, et - st
