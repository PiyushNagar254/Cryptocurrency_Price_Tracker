# import boto3
# from io import StringIO
# #  for uploading AWS
# def upload_to_s3(pandas_df, bucket, file_path, aws_access_key_id, aws_secret_access_key):
#     # Initialize the AWS session
#     session = boto3.Session(
#         aws_access_key_id=aws_access_key_id,
#         aws_secret_access_key=aws_secret_access_key
#     )
#     s3_client = session.client('s3')
#
#     # Convert DataFrame to CSV in-memory
#     csv_buffer = StringIO()
#     pandas_df.to_csv(csv_buffer, index=False)
#
#     # Upload to S3
#     s3_client.put_object(
#         Bucket=bucket,
#         Key=file_path,
#         Body=csv_buffer.getvalue()
#     )
#     print(f"File uploaded to S3: {file_path}")


# Import upload_to_s3 from aws.py for immediate data upload after saving
# from aws import upload_to_s3
import boto3
from io import StringIO
import os
import pandas as pd
from dotenv import load_dotenv

# Loading environment variables
load_dotenv()

# Function to upload DataFrame to AWS S3
def upload_to_s3(pandas_df, bucket, file_path, aws_access_key_id, aws_secret_access_key):
    # Initialize the AWS session
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    s3_client = session.client('s3')

    # Converting DataFrame to CSV in-memory
    csv_buffer = StringIO()
    pandas_df.to_csv(csv_buffer, index=False)

    # Upload to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=file_path,
        Body=csv_buffer.getvalue()
    )
    print(f"File uploaded to S3: {file_path}")

def main():
    # Assume df is fetched or passed from crypto.py
    df = pd.read_csv('path_to_your_local_csv_file.csv')  # Placeholder for your DataFrame
    bucket_name = os.getenv('S3_BUCKET')  # S3 bucket name
    file_path = f'cryptocurrency_data_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

    upload_to_s3(df, bucket_name, file_path, aws_access_key_id, aws_secret_access_key)

if __name__ == '__main__':
    main()
