from dotenv import load_dotenv
import os
import boto3
import json

#Load from .env file
load_dotenv()

with open("/Cloud_Data_Lake/config/pipeline_config.json") as f:
    conf_data = json.load(f)

access_keyid = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
default_region = os.getenv("AWS_DEFAULT_REGION")
role_arn = os.getenv("AWS_ROLE_ARN")


#Connect to AWS S3 bucket and upload data
def create_s3_session(access_key, secret_key, dflt_rgn):
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=dflt_rgn,
    )
    s3 = session.resource("s3")
    return s3

def upload_data_s3():
    s3_resource = create_s3_session(access_keyid, aws_secret_key, default_region)
    