import boto3
import os
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

load_dotenv()

# Retrieve the AWS credentials from environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

# Initialize the S3 client with credentials from environment variables
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)


# Function to upload a file to S3
def upload_to_s3(file_path, bucket_name, object_name):
    try:
        s3.upload_file(file_path, bucket_name, object_name)
        print(f"Upload Successful: {object_name}")
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except NoCredentialsError:
        print("Credentials not available")


# Main function to find and upload the data files from the folders
def upload_files_from_folders(base_directory, bucket_name, folder_names):
    for folder in folder_names:
        folder_path = os.path.join(base_directory, folder)

        # Iterate through the files in the folder
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)

            # Check if the file is a CSV file
            if file_name.endswith('.csv'):
                object_name = f"{folder}/{file_name}"  # Use folder name to maintain structure in S3
                print(f"Uploading {file_path} to S3 as {object_name}...")
                upload_to_s3(file_path, bucket_name, object_name)


# Directories to upload
base_directory = '/Users/piyushnagar/PycharmProjects/cryptoaws'
bucket_name = 'piyush-crypto-currency'

# Folders to upload
folder_names = [
    "Final_data_for_aws",
    "Cleaned_filtered_data"
]

# Run the upload process
upload_files_from_folders(base_directory, bucket_name, folder_names)
