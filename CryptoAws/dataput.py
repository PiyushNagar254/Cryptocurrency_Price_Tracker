from dotenv import load_dotenv
import boto3
import os
from botocore.exceptions import NoCredentialsError
# Load environment variables from the .env file
load_dotenv()  # This automatically loads variables from the .env file into the environment

# Retrieve the AWS credentials from environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

# Initialize the S3 client using the loaded credentials
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

def upload_to_s3(file_name, bucket_name, object_name=None):

    if object_name is None:
        object_name = file_name

    try:
        s3.upload_file(file_name, bucket_name, object_name)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

file_name = '/Users/piyushnagar/downloads/all_currencies.csv'  # path to your file
bucket_name = 'piyush-crypto-currency'  # S3 bucket name
object_name = 'all_currencies.csv'  # S3 object name

upload_to_s3(file_name, bucket_name, object_name)