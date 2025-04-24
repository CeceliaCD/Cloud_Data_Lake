from dotenv import load_dotenv
import os
import boto3
import json

#Load from .env file
load_dotenv()

with open("/Cloud_Data_Lake/config/pipeline_config.json") as f:
    conf_data = json.load(f)

access_keyid = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
role_arn = os.getenv("AWS_ROLE_ARN")

s3 = boto3.resource("s3")
#Connect to AWS S3 bucket and upload data
def create_s3_client():
    boto3.client()

def create_s3_session():
    boto3.Session()