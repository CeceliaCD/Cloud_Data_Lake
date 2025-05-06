import sys,os
import boto3
if "/tmp" not in sys.path:
    sys.path.insert(0, "/tmp")
from src.etl.extraction_job import extract_s3_data
from src.etl.transformation_job import transform_data
from src.etl.data_loader_job import load_s3_data
from src.queries.athena_runner import run_athena_queries
from utils.config_loader import load_json_config
import datetime as datetime
import logging
logger = logging.getLogger(__name__)
"""
abs_path_file = os.path.abspath(__file__)
curr_dir = os.path.dirname(abs_path_file)
parent_dir = os.path.dirname(curr_dir)
sys.path.insert(0, parent_dir)
"""
if "/tmp" not in sys.path:
    sys.path.insert(0, "/tmp")
config_path = '/tmp/config/datalake_pipeline_config.json' if os.path.exists('/tmp/config/datalake_pipeline_config.json') else os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'datalake_pipeline_config.json')
config = load_json_config(config_path)
from dotenv import load_dotenv
load_dotenv(override=True)

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
    #print("Retrieved jobname, input and output")
    
    s3_client = get_s3_client()
    
    
    if job_name == config["aws_glue"]["etl_jobs"][0]["name"]:
        load_s3_data(s3_client, os.path.join('tmp', 'sample_data', config["data_source"]["csv_file"]), config["s3_bucket"]["raw_prefix"])
        
        rd_df = extract_s3_data(s3_client, input_path)
        
        cleaned_df = transform_data(rd_df)
        
        load_s3_data(s3_client, cleaned_df, output_path)
    elif job_name == config["aws_glue"]["etl_jobs"][1]["name"]:
        athena_client = get_athena_client()
        
        processed_df = extract_s3_data(s3_client, input_path)
        
        run_athena_queries(athena_client, processed_df, output_path, acct_id)
        athena_client.close()
    
if __name__ == '__main__':
    main()