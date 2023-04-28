import boto3
import json
from time import sleep, time
from zipfile import ZipFile
from os import path

LAMBDA_NAMES = ['coordinator', 'count', 'distinct', 'filter', 'first', 'group_by_key', 'group_by_value', 'intersection', 'map',
                'reduce_by_key', 'reduce', 'take', 'union', 'take_ordered', 'sort', 'coordinator2', 'map2', 'reduce_by_key2']

REGION = "eu-central-1"

iam = boto3.client('iam')
lambda_client = boto3.client('lambda', region_name=REGION)
s3_client = boto3.client('s3', region_name=REGION)
dynamo_client = boto3.resource(service_name='dynamodb', region_name=REGION)


def extract_payload(resp):
    payload = json.loads(resp['Payload'].read())
    try:
        result = payload['body']
    except Exception as e:
        print("exception while parsing the payload body: " + str(e))
        result = payload

    return result


def zip_code(lambda_name, update_lambdas):
    if update_lambdas or (not path.isfile(lambda_name + '.zip')):
        with ZipFile(lambda_name + '.zip', 'w') as zip_file:
            zip_file.write(lambda_name + '.py')
            zip_file.close()

    with open(lambda_name + '.zip', 'rb') as f:
        zipped_code = f.read()
        return zipped_code


def create_lambda_function(lambda_name, zipped_code, role, update_lambdas):
    try:
        print("creating \'" + lambda_name + "\' lambda function...")

        resp = lambda_client.create_function(
            FunctionName=lambda_name,
            Runtime='python3.9',
            Role=role['Role']['Arn'],
            Handler=lambda_name + '.lambda_handler',
            Code=dict(ZipFile=zipped_code),
            Timeout=300,  # Maximum allowable timeout
        )

        if resp:
            print("successfully created lambda function")

    except lambda_client.exceptions.ResourceConflictException:
        print("lambda already exists")
        if update_lambdas:
            print("updating lambda code...")
            lambda_client.update_function_code(FunctionName=lambda_name, ZipFile=zipped_code)


def create_s3_bucket(bucket_name):
    try:
        print("creating s3 input bucket...")
        location = {'LocationConstraint': REGION}
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)

    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        print("bucket already exists")


def init(update_lambdas):
    basic_lambda_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {
                    "Service": "lambda.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

    try:
        print("creating DynamoDB tables...")
        print("intermediate1")
        table = dynamo_client.create_table(
            TableName="intermediate1",
            KeySchema=[{
                'AttributeName': 'id',
                'KeyType': 'HASH'
            }],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'N'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        table.wait_until_exists()

        print("intermediate2")
        table = dynamo_client.create_table(
            TableName="intermediate2",
            KeySchema=[{
                'AttributeName': 'id',
                'KeyType': 'HASH'
            }],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'N'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        table.wait_until_exists()

        print("intermediate3")
        table = dynamo_client.create_table(
            TableName="intermediate3",
            KeySchema=[{
                'AttributeName': 'id',
                'KeyType': 'HASH'
            }],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'N'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        table.wait_until_exists()
        print("all tables created")

    except Exception as e:
        print("Error while creating dynamo tables: " + str(e))

    try:
        print("creating LambdaBasicExecution iam role...")
        iam.create_role(
            RoleName='LambdaBasicExecution',
            AssumeRolePolicyDocument=json.dumps(basic_lambda_policy),
        )

    except iam.exceptions.EntityAlreadyExistsException:
        print("role already exists")

    try:
        print("attaching S3FullAccess policy to iam role...")
        iam.attach_role_policy(
            RoleName='LambdaBasicExecution',
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess',
        )

    except Exception as e:
        print("Unexpected error during policy attachment: " + str(e))

    try:
        print("attaching DynamoDB policy to iam role...")
        iam.attach_role_policy(
            RoleName='LambdaBasicExecution',
            PolicyArn='arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess',
        )

    except Exception as e:
        print("Unexpected error during policy attachment: " + str(e))

    try:
        print("attaching CloudWatchLogsFullAccess policy to iam role...")
        iam.attach_role_policy(
            RoleName='LambdaBasicExecution',
            PolicyArn='arn:aws:iam::aws:policy/CloudWatchLogsFullAccess',
        )

    except Exception as e:
        print("Unexpected error during policy attachment: " + str(e))

    try:
        print("creating CoordinatorLambdaExecution iam role...")
        iam.create_role(
            RoleName='CoordinatorLambdaExecution',
            AssumeRolePolicyDocument=json.dumps(basic_lambda_policy),
        )

    except iam.exceptions.EntityAlreadyExistsException:
        print("role already exists")

    try:
        print("attaching S3FullAccess policy to iam role...")
        iam.attach_role_policy(
            RoleName='CoordinatorLambdaExecution',
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess',
        )

    except Exception as e:
        print("Unexpected error during policy attachment: " + str(e))

    try:
        print("attaching DynamoDB policy to iam role...")
        iam.attach_role_policy(
            RoleName='CoordinatorLambdaExecution',
            PolicyArn='arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess',
        )

    except Exception as e:
        print("Unexpected error during policy attachment: " + str(e))

    try:
        print("attaching AWSLambdaRole policy to iam role...")
        iam.attach_role_policy(
            RoleName='CoordinatorLambdaExecution',
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaRole',
        )

    except Exception as e:
        print("Unexpected error during policy attachment: " + str(e))

    try:
        print("attaching CloudWatchLogsFullAccess policy to iam role...")
        iam.attach_role_policy(
            RoleName='CoordinatorLambdaExecution',
            PolicyArn='arn:aws:iam::aws:policy/CloudWatchLogsFullAccess',
        )

    except Exception as e:
        print("Unexpected error during policy attachment: " + str(e))

    lambda_role = iam.get_role(RoleName='LambdaBasicExecution')
    coordinator_role = iam.get_role(RoleName='CoordinatorLambdaExecution')

    print("synchronizing lambdas...")

    for name in LAMBDA_NAMES:
        if name == 'coordinator':
            role = coordinator_role
        else:
            role = lambda_role

        zipped_code = zip_code(name, update_lambdas)
        create_lambda_function(name, zipped_code, role, update_lambdas)

    print("all lambdas synchronized...")
    print("waiting 3 seconds to ensure lambdas deployment...")

    sleep(3)

    print("successfully initialized")

    # data = json.dumps(
    #     {
    #         "lambdas": [
    #             {'name': 'map', 'func': 'lambda x: x*2'},
    #             {'name': 'filter', 'func': 'lambda x: x<4'},
    #             {'name': 'first'}
    #         ],
    #         "data": [1,
    #                 2,
    #                 3,
    #                 4,
    #                 5,
    #                 6],
    #     }
    # )


def invoke_coordinator(data):
    print("invoking coordinator...")
    st = time()

    response = lambda_client.invoke(
        FunctionName='coordinator',
        Payload=data,
        LogType='Tail')

    et = time()

    result = extract_payload(response)
    elapsed = et - st
    print("Processing executed in " + str(elapsed) + " seconds")
    return result, elapsed

# dane przetwarzamy w batchach
# po kazdej funkcji dzielimy i skladamy ale nie przed kazda warto skladac -> analiza kiedy skladac a kiedy nie
# wykorzystanie s3
# dokonczyc reszte funkcji
# przekmina odnosnie ilosci koordynatorow i samych koordynatorow
# -> co jesli koordynator padnie
# -> jak przekazac innej replikacji dane, ktore on przechowywal (ktora funkcje wywolac, gdzie znajduja sie dane)
# -> jak odtworzyc dzialanie koordynatora na replikacji


# testy na innej bazce
# proby zoptymalizowania przetwarzania -> co jesli np map bedzie trwal za dlugo
# czy da sie jakas lambda kooperowac zeby to trwalo lepiej
# czy moze rozbic to na jakies pipeline'y
# albo w jednym porozbijac dalej rownolegle dane
# generalnie podrasowanie wydajnosciowe

# plm dział
# od polowy lutego chyba 13 akademia <- nauka pod tamtejsza praca 2-3 miesiace
# aptive firma 
# ttrp 1,5tyg


# wiecej scenariuszy!
# porownac z innym frameworkiem
# umozliwic konfiguracje np batchow i uwzglednic w scenariuszach
# zautomatyzowac wykonanie scenariuszow dla roznych konfiguracji i automatycznie wygenerowac raport


# hadoop i wszystkie testy
# dostosowac testy i batch numbery na sparku zeby bylo max analogicznie
# wygenerowac wykresiki i bedziemy wnioskowac ostro
# projekcik na overlafie i moze juz jakis spis tresci
#templatki na stronie wydzialu lub na stronie sobanca