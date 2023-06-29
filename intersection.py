import json
import boto3


def extract_body(response):
    payload = json.loads(response['Body'].read())
    return payload


def lambda_handler(event, _):
    s3_client = boto3.client('s3', region_name="eu-central-1")
    keys = event['id']
    s3_data = []
    for k in keys:
        data = s3_client.get_object(Bucket='intermediate1-sbg-bucket', Key=str(k))
        data = extract_body(data)
        s3_data.append(data)
    result = list(set.intersection(*map(set, s3_data)))

    return {
        'statusCode': 200,
        'body': result
    }
