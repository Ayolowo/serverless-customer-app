import logging
import boto3
import boto3.dynamodb
import botocore
from botocore.exceptions import ClientError

# Create S3 bucket for movie images
def create_bucket(movies_images, region='us-east-1'):
    try:
        if region is None:
            s3_client = boto3.resource('s3')
            s3_client.create_bucket(movies_images)
        else:
            s3_client = boto3.resource('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=movies_images, CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True


# Retrieve the List of existing bucket names
s3 = boto3.client('s3')
response = s3.list_buckets()

# Output the bucket names
print('Existing buckets:')
for bucket in response['Buckets']:
    print(f'  {bucket["Name"]}')

# To upload a file to the S3 bucket
def upload_file(file_name, movies_images, object_name=None):
    # If S3 Object_name isn't specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, movies_images, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


# DynamoDB table creation

dynamodb = boto3.resource('dynamodb')

table = dynamodb.create_table(
    TableName='users',
    KeySchema=[
        {
            'AttributeName': 'title',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'genre',
            'KeyType': 'RANGE'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'title',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'releaseYear',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'genre',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'coverUrl',
            'AttributeType': 'S'
        },
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
)

# Wait until the table exists.
table.wait_until_exists()

# Print out some data about the table.
print(table.item_count)

# Place items into the dynamoDB table
item_1 = {'title': 'gemini man','releaseYear': 2004,'genre': 'Science Fiction, Action', 'coverUrl':'www.url.com'}
item_2 = {'title': 'deadpool', 'releaseYear': 2024, 'genre': 'Action, Blood, humour, Violence'}

items_to_add = [item_1, item_2]

with table.batch_wrtiter() as batch:
    for item in items_to_add:
        batch.put_item(Item=items_to_add)