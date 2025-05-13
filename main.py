import sys
import boto3
import json
from pokemonetl.etl.extraction_job import extract_s3_data
from pokemonetl.etl.transformation_job import transform_data
from pokemonetl.etl.data_loader_job import load_s3_data
from pokemonetl.queries.athena_runner import run_athena_queries
import datetime as datetime
import logging
logger = logging.getLogger(__name__)
import importlib.resources as pkg_resources
import config

config_path = pkg_resources.files(config).joinpath("datalake_pipeline_config.json")

with config_path.open('r', encoding='utf-8') as f:
    config_data = json.load(f)
    
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
    
    if job_name == config_data["aws_glue"]["etl_jobs"][0]["name"]:
        #Loading raw data to raw zone
        #filename = 'sample_data/pokemon_data.csv'
        #load_s3_data(s3_client, filename, input_path)
        
        #Extract
        rd_df = extract_s3_data(s3_client, input_path)
        
        #Transform
        cleaned_df = transform_data(rd_df)
        
        #Load
        load_s3_data(s3_client, cleaned_df, output_path)
    elif job_name == config_data["aws_glue"]["etl_jobs"][1]["name"]:
        athena_client = get_athena_client()
        
        run_athena_queries(athena_client, input_path, output_path, acct_id)
    
if __name__ == '__main__':
    main()