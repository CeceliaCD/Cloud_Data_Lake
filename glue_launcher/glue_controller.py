import boto3
import pandas as pd
import datetime as datetime
import time
import sys, os
from dotenv import load_dotenv
load_dotenv()
abs_path_file = os.path.abspath(__file__)
curr_dir = os.path.dirname(abs_path_file)
parent_dir = os.path.dirname(curr_dir)
sys.path.insert(0, parent_dir)
from utils.config_loader import load_json_config

config = load_json_config("config/datalake_pipeline_config.json")
access_keyid = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
default_region = os.getenv("AWS_DEFAULT_REGION")
glue_arn = os.getenv("GLUE_ROLE_ARN")
region_name = os.getenv("AWS_DEFAULT_REGION")
profile = os.getenv("PROFILE_NAME")
acct_id = os.getenv("ACCOUNT_ID")

"""_summary_

Glue Crawler job to Scan Processed Zone and Update Catalog

Run Athena Query  to get Curated data then Store in "curated/"

Connect Tableau to Athena for visualization
"""

#Connect to AWS S3 bucket and upload data
def create_aws_session(access_key, secret_key, dflt_rgn, prfl_nm, acct):
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=dflt_rgn,
        profile_name=prfl_nm,
        aws_account_id=acct
    )
    return session

#Start n glue crawler
def start_glue_crawler_job(client, etl_job_name, arguments):
    client.start_crawler(Name=config["aws_glue"]["crawler_name"])
    
    while True:
        response = client.get_crawler(Name=config["aws_glue"]["crawler_name"])      
        status = response['Crawler']['State']
        print(f"Crawler status: {status}")
        if status == 'READY':
            break
        time.sleep(10)
        
    glue_response= client.start_job_run(JobName= etl_job_name,
                                Arguments=arguments)
        
    return glue_response['JobRunId']

#wait for job completion status
def wait_for_job_completion(client, etl_job_name, run_id):
    while True:
        glue_response = client.get_job_run(JobName=etl_job_name, RunId=run_id)
        status = glue_response['JobRun']['JobRunState']
        print(f"Job {etl_job_name} (Run ID: {run_id}) status: {status}")
        if status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
            break
        time.sleep(10)
    return status

def run_etl_pipeline():
    aws_session = create_aws_session(access_keyid, aws_secret_key, region_name, profile, acct_id)
    glue_client = aws_session.client('glue')
    #s3_client = aws_session.client('s3')
    #athena_client = aws_session.client('athena')
    try:
        glue_client.create_database(DatabaseInput={
        'Name': config["aws_glue"]["catalog"],
        'Description': 'Catalog for processed and curated data zone of pokemon data.',
        'LocationUri': str(config["s3_bucket"]["bucket"] + config["s3_bucket"]["processed_prefix"])
        })
        print(f"Created database: {config["aws_glue"]["catalog"]}")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"Database {config["aws_glue"]["catalog"]} already exists.")
    
    try:
        glue_client.create_crawler( 
            Name=config["aws_glue"]["crawler_name"],
            Role=glue_arn, 
            DatabaseName=config["aws_glue"]["catalog"],
            Targets={
                'S3Targets': [
                    {'Path': str(config["s3_bucket"]["bucket"] + config["s3_bucket"]["processed_prefix"])}
                ]
            },
            TablePrefix= config["s3_bucket"]["processed_prefix"],
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
            },
            Description='Crawler for processed data zone in S3.')
    except glue_client.exceptions.AlreadyExistsException:
        print(f"Crawler for database {config["aws_glue"]["catalog"]} already exists.")
    
    # Stage 1: raw to processed    
    raw_run_id = start_glue_crawler_job(
        glue_client,
        config["etl_jobs"][0]["name"],
        {
            '--input_path': str(config["s3_bucket"]["bucket"] + config["s3_bucket"]["raw_prefix"] + config["data_source"]["csv_file"]),
            '--output_path': str(config["s3_bucket"]["bucket"] + config["s3_bucket"]["processed_prefix"] + config["data_source"]["processed_file"]),
        }
    )
    status1 = wait_for_job_completion(glue_client, config["etl_jobs"][0]["name"], raw_run_id)

    # Stage 2: processed to curated
    curated_run_id = start_glue_crawler_job(
        glue_client,
        config["etl_jobs"][1]["name"],
        {
            '--input_path': str(config["s3_bucket"]["bucket"] + config["s3_bucket"]["processed_prefix"] + config["data_source"]["processed_file"]),
            '--output_path': str(config["s3_bucket"]["bucket"] + config["s3_bucket"]["curated_prefix"] + config["data_source"]["curated_file"])
        }
    )
    status2 = wait_for_job_completion(glue_client, config["etl_jobs"][1]["name"], curated_run_id)
    print(f"ETL pipeline {status2}!")
    glue_client.close()

if __name__ == '__main__':
    run_etl_pipeline()