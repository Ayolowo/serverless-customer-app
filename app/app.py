import logging
import boto3
from botocore.exceptions import ClientError

# Create S3 bucket for movie images
def create_bucket(movies_images, region=us-east-1):
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
