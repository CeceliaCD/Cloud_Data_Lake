import boto3
import json
from dotenv import load_dotenv
load_dotenv()
import os, sys
abs_path_file = os.path.abspath(__file__)
curr_dir = os.path.dirname(abs_path_file)
parent_dir = os.path.dirname(curr_dir)
sys.path.insert(0, parent_dir)
from utils.config_loader import load_json_config

config_file = load_json_config("config/datalake_pipeline_config.json")

#Setting up logging
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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

def load_s3_data():
    s3_resource = create_s3_session(access_keyid, aws_secret_key, default_region)
    
def load_catalog_data():
    pass
    