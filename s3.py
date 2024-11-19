import boto3
import boto3.dynamodb
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import logging
import os


# Used to track/log events that happen as your program runs
logger = logging.getLogger(__name__)

# Create S3 bucket for movie images
#bucket_name = input("Plese input your bucket_name: ")
#region = input("Please input desired region: ")
def bucket_exists(bucket_name, region='us-east-1'):
    s3 = boto3.client('s3')
    try:
        s3.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Bucket '{bucket_name}' does not exist.")
        else:
            # Log any other error
            print(f"Error checking bucket existence: {e}")
        return False

def create_s3_bucket(bucket_name, region='us-east-1'):
    # Call the bucket_exists function to check for existence
    if bucket_exists(bucket_name, region='us-east-1'):
        print(f"Bucket '{bucket_name}' already exists in '{region}'")
        return True
    
    try:
        s3 = boto3.client('s3', region_name=region)
        response = s3.create_bucket(Bucket=bucket_name)
        print("Bucket created successfully with the following response:")
        print(f"Bucket '{bucket_name}' was created in the '{region}' region.") # type: ignore

    except ClientError as e:
        logging.error(e)
        return False
    return True

# Check if file already exists in the s3 bucket
def file_exists_in_s3(bucket_name, object_name):
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=bucket_name, Key=object_name)
        print(f"File '{object_name}' already exists in bucket '{bucket_name}'.")
        return True
    
    except ClientError as e:
        # If the error is not '404 Not Found', log it
        if e.response['Error']['Code'] != '404':
            logging.error(e)
        return False

# To upload a file to the S3 bucket
def upload_file(file_name, bucket_name, object_name):
    if not file_name:
        print("Error: No file name provided.")
        return False

    if not os.path.isfile(file_name):
        print(f"Error: File '{file_name}' does not exist.")
        return False
    
    if file_exists_in_s3(bucket_name, object_name):
        print(f"Skipping upload for '{file_name}' as it already exists.")
        return False
    
    if object_name is None:
        object_name = os.path.basename(file_name)
        
    try:
        s3 = boto3.client('s3')
        s3.upload_file(file_name, bucket_name, object_name)
        print(f"File '{file_name}' uploaded successfully to '{bucket_name}/{object_name}'")
    except ClientError as e:
        logging.error(e)
        return False
    return True

# To upload all files in the s3 bucket
def upload_all_files(file_list, bucket_name):
    for file_name in file_list:
        object_name = os.path.basename(file_name)
        success = upload_file(file_name, bucket_name, object_name)
        if not success:
            print(f"Failed to upload '{file_name}' or it already exists.")
        else:
            print(f"'{file_name}' uploaded successfully.")

class S3BucketManager:
    def __init__(self, bucket_name, region='us-east-1'):
        self.bucket_name = bucket_name
        self.s3 = boto3.client('s3', region_name=region)

    def delete_bucket(self):
        confirm = input(f"Do you want to delete the bucket '{self.bucket_name}'? (yes/no): ").strip().lower()
        if confirm != 'yes':
            print("Bucket deletion canceled.")
            return
        
        try:
            self.s3.delete_bucket(Bucket=self.bucket_name)
            print("Bucket deleted successfully")
        except ClientError as err:
            logger.error(
                "Couldn't delete bucket. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
    
# Call the function to create the bucket
if __name__ == "__main__":
    bucket_name='my-customer-data-bucket'
    file_list = ['CUST001_transactions.csv', 'CUST002_transactions.csv', 'CUST003_transactions.csv']
    create_s3_bucket(bucket_name)
    upload_all_files(file_list, bucket_name)
    bucket_manager = S3BucketManager(bucket_name)
    bucket_manager.delete_bucket()


