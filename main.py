import sys,os
import boto3
import pandas as pd
from src.etl.extraction_job import extract_s3_data
from src.etl.transformation_job import transform_data
from src.etl.data_loader_job import load_s3_data
from src.queries.athena_runner import run_athena_queries
from utils.config_loader import load_json_config
import datetime as datetime
import logging
logger = logging.getLogger(__name__)
config = load_json_config('config/datalake_pipeline_config.json')
from dotenv import load_dotenv
load_dotenv()

acct_id = os.getenv("ACCOUNT_ID")
try:
    from awsglue.utils import getResolvedOptions
except ImportError:
    def getResolvedOptions(args, options):
        # Manually providing expected values for testing
        return {
            'JOB_NAME': 'raw_to_processed_job',
            's3_input_path': 's3://my-bucket/raw/',
            's3_output_path': 's3://my-bucket/processed/'
        }
        
def get_s3_client():
    """Create and return an S3 client."""
    return boto3.client('s3')   

def get_athena_client():
    """Create and return an athena client."""
    return boto3.client('athena')   

def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'output_path'])
    
    job_name = args['JOB_Name']
    input_path = args['input_path']
    output_path = args['output_path']
    
    s3_client = get_s3_client()
    athena_client = get_athena_client()
    
    if job_name == config["etl_jobs"][0]["name"]:
        load_s3_data(s3_client, str('sample_data/' + config["data_source"]["csv_path"]), input_path)
        
        rd_df = extract_s3_data(s3_client, input_path)
        
        cleaned_df = transform_data(rd_df)
        
        load_s3_data(s3_client, cleaned_df, output_path)
    elif job_name == config["etl_jobs"][1]["name"]:
        
        processed_df = extract_s3_data(s3_client, input_path)
        
        run_athena_queries(athena_client, processed_df, output_path, acct_id)
    
if __name__ == '__main__':
    main()