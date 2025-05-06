import sys,os
import boto3
from src.etl.extraction_job import extract_s3_data
from src.etl.transformation_job import transform_data
from src.etl.data_loader_job import load_s3_data
from src.queries.athena_runner import run_athena_queries
from utils.config_loader import load_json_config
import datetime as datetime
from dotenv import load_dotenv
load_dotenv(override=True)
import logging
logger = logging.getLogger(__name__)
import importlib.resources as pkg_resources
import config

with pkg_resources.open_text(config, "datalake_pipeline_config.json") as f:
    config_file = load_json_config(f)

try:
    from awsglue.utils import getResolvedOptions
except ImportError:
    def getResolvedOptions(args, options):
        # Job values will go in here
        args = {
            '--conf': options[0],
            '--JOB_NAME': options[1],
            '--input_loc': options[2],
            '--output_loc': options[3]
        }
        return args
        
def get_s3_client():
    """Create and return an S3 client."""
    return boto3.client('s3')   

def get_athena_client():
    """Create and return an athena client."""
    return boto3.client('athena')   

def main():
    args = getResolvedOptions(sys.argv, ['conf', 'JOB_NAME', 'input_loc', 'output_loc'])
    
    acct_id = args['conf']
    job_name = args['JOB_NAME']
    input_path = args['input_loc']
    output_path = args['output_loc']
    
    s3_client = get_s3_client()
    
    
    if job_name == config_file["aws_glue"]["etl_jobs"][0]["name"]:
        csv_file = 'sample/' + config_file["data_source"]["csv_file"]
        load_s3_data(s3_client, csv_file, config_file["s3_bucket"]["raw_prefix"])
        
        rd_df = extract_s3_data(s3_client, input_path)
        
        cleaned_df = transform_data(rd_df)
        
        load_s3_data(s3_client, cleaned_df, output_path)
    elif job_name == config_file["aws_glue"]["etl_jobs"][1]["name"]:
        athena_client = get_athena_client()
        
        processed_df = extract_s3_data(s3_client, input_path)
        
        run_athena_queries(athena_client, processed_df, output_path, acct_id)
        athena_client.close()
    
if __name__ == '__main__':
    main()