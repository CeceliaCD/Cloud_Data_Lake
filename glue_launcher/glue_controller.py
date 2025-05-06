import boto3
import datetime as datetime
import time
import sys, os
import json
from dotenv import load_dotenv
load_dotenv(override=True)
abs_path_file = os.path.abspath(__file__)
curr_dir = os.path.dirname(abs_path_file)
parent_dir = os.path.dirname(curr_dir)
sys.path.insert(0, parent_dir)
import importlib.resources as pkg_resources
import config

with pkg_resources.open_text(config, "datalake_pipeline_config.json") as f:
    config_file = json.load(f)
    
access_keyid = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
default_region = os.getenv("AWS_DEFAULT_REGION")
glue_arn = os.getenv("GLUE_ROLE_ARN")
region_name = os.getenv("AWS_DEFAULT_REGION")
#profile = os.getenv("PROFILE_NAME")
acct_id = os.getenv("ACCOUNT_ID")

"""_summary_

Glue Crawler job to Scan Processed Zone and Update Catalog

Run Athena Query  to get Curated data then Store in "curated/"

Connect Tableau to Athena for visualization
"""

#Connect to AWS S3 bucket and upload data
def create_aws_session(access_key, secret_key, dflt_rgn): #prfl_nm, acct
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=dflt_rgn
        #profile_name=prfl_nm,
        #aws_account_id=acct
    )
    return session  

#Start glue job
def start_glue_job(client, etl_job_name, arguments):
    
    #crawler is started in job
    glue_response= client.start_job_run(JobName=etl_job_name,
                                Arguments=arguments)
        
    return glue_response['JobRunId']

#wait for job completion status
def wait_for_job_and_crawler_completion(client, etl_job_name, run_id):
    while True:
        glue_response = client.get_job_run(JobName=etl_job_name, RunId=run_id)
        status = glue_response['JobRun']['JobRunState']
        print(f"Job {etl_job_name} (Run ID: {run_id}) status: {status}")
        if status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
            break
        time.sleep(10)
        
    client.start_crawler(Name=config_file["aws_glue"]["crawler_name"])
    while True:
        response = client.get_crawler(Name=config_file["aws_glue"]["crawler_name"])      
        status = response['Crawler']['State']
        print(f"Crawler status: {status}")
        if status == 'READY':
            break
        time.sleep(10)

    print(f"Crawler {response['Crawler']['Name']} finished running...")
    return status

def run_etl_pipeline():
    aws_session = create_aws_session(access_keyid, aws_secret_key, region_name) #'default', acct_id
    glue_client = aws_session.client('glue')
    try:
        glue_client.create_database(DatabaseInput={
        'Name': config_file["aws_glue"]["catalog"],
        'Description': 'Catalog for processed and curated data zone of pokemon data.',
        'LocationUri': str(config_file["s3_bucket"]["bucket"] + config_file["s3_bucket"]["processed_prefix"]),
        'CreateTableDefaultPermissions': [
            {
                'Principal': {
                    'DataLakePrincipalIdentifier': glue_arn
                },
                'Permissions': ['ALL']
            },
            ]
        } 
        )
        print(f"Created database: {config_file['aws_glue']['catalog']}")
    except Exception as e:
        print(f"Database {config_file['aws_glue']['catalog']} already exists.")
    
    try:
        glue_client.create_crawler( 
            Name=config_file["aws_glue"]["crawler_name"],
            Role=glue_arn, 
            DatabaseName=config_file["aws_glue"]["catalog"],
            Targets={
                'S3Targets': [
                    {'Path': str(config_file["s3_bucket"]["bucket"] + config_file["s3_bucket"]["processed_prefix"])}
                ]
            },
            TablePrefix= config_file["s3_bucket"]["processed_prefix"],
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
            },
            Description='Crawler for processed data zone in S3.')
    except Exception as e:
        print(f"Crawler for database {config_file['aws_glue']['catalog']} already exists.")
    
    try:
        existing_job = glue_client.get_job(JobName=config_file["aws_glue"]["etl_jobs"][0]["name"])
        print(f"Job {existing_job['Job']['Name']} already exists. Skipping creation.")
    except glue_client.exceptions.EntityNotFoundException:
        response = glue_client.create_job(
            Name=config_file["aws_glue"]["etl_jobs"][0]["name"],
            Role=glue_arn,  # Replace with your IAM role ARN
            JobMode='SCRIPT',
            ExecutionProperty={'MaxConcurrentRuns': 1},
            Command={
                'Name': 'pythonshell',
                'ScriptLocation': config_file["aws_glue"]["S3_URI"],  
                'PythonVersion': '3.9'
            },
            DefaultArguments={
                '--TempDir': str(config_file["s3_bucket"]["bucket"] + config_file["s3_bucket"]["temp"]),
                '--extra-py-files': str(config_file["s3_bucket"]["bucket"] + config_file["s3_bucket"]["dependencies"] + 'pokemon_datalake_and_anltx-0.1.0-cp39-none-any.whl'),
                '--job-language': 'python',
                '--additional-python-modules': 'pandas,requests,pyarrow,avro-python3'
            },
            MaxRetries=0,
            GlueVersion='1.0',
            Description='Job for processing raw Pokémon data.'
        )
        print(f"Job has been created: {response['Name']}")

    # Stage 1: raw to processed    
    raw_run_id = start_glue_job(
        glue_client,
        config_file["aws_glue"]["etl_jobs"][0]["name"],
        {
            '--conf': acct_id,
            '--JOB_NAME': config_file["aws_glue"]["etl_jobs"][0]["name"],
            '--input_loc': str(config_file["s3_bucket"]["bucket"] + config_file["s3_bucket"]["raw_prefix"] + config_file["data_source"]["csv_file"]),
            '--output_loc': str(config_file["s3_bucket"]["bucket"] + config_file["s3_bucket"]["processed_prefix"]), 
        }
    )
    status1 = wait_for_job_and_crawler_completion(glue_client, config_file["aws_glue"]["etl_jobs"][0]["name"], raw_run_id)
    """
    glue_client.update_job(
        JobName=config["aws_glue"]["etl_jobs"][1]["name"],
        JobUpdate={
            'Role': glue_arn,
            'Command': {
                'Name': 'glueetl',
                'ScriptLocation': config["aws_glue"]["S3_URI"],
                'PythonVersion': '3'
            },
            # other updated fields
        }
    )
    """
    curated_run_id = None
    if status1 == 'SUCCEEDED':
        # Stage 2: processed to curated
        response2 = glue_client.create_job(
            Name=config_file["aws_glue"]["etl_jobs"][1]["name"],
            Role=glue_arn,  # Replace with your IAM role ARN
            JobMode='SCRIPT',
            ExecutionProperty={'MaxConcurrentRuns': 1},
            Command={
                'Name': 'pythonshell',
                'ScriptLocation': config_file["aws_glue"]["S3_URI"],  
                'PythonVersion': '3.9'
            },
            DefaultArguments={
                '--TempDir': str(config_file["s3_bucket"]["bucket"] + config_file["s3_bucket"]["temp"]),
                '--extra-py-files': str(config_file["s3_bucket"]["bucket"] + config_file["s3_bucket"]["dependencies"] + 'pokemon_datalake_and_anltx-0.1.0-cp39-none-any.whl'),
                '--job-language': 'python',
                '--additional-python-modules': 'pandas,requests,pyarrow,avro-python3'
            },
            MaxRetries=0,
            GlueVersion='1.0',
            Description='Job for curating processed Pokémon data.'
        )
        print(f"Job has been created: {response2['Name']}")
        
        curated_run_id = start_glue_job(
            glue_client,
            config_file["aws_glue"]["etl_jobs"][1]["name"],
            {
                '--conf': acct_id,
                '--JOB_NAME': config_file["aws_glue"]["etl_jobs"][1]["name"],
                '--input_loc': str(config_file["s3_bucket"]["bucket"] + config_file["s3_bucket"]["processed_prefix"] + config_file["data_source"]["processed_file"]),
                '--output_loc': str(config_file["s3_bucket"]["bucket"] + config_file["s3_bucket"]["curated_prefix"]) 
            }
        )
    status2 = None
    if curated_run_id == None:
        glue_client.close()
    else:
        status2 = wait_for_job_and_crawler_completion(glue_client, config_file["aws_glue"]["etl_jobs"][1]["name"], curated_run_id)
    
    if status2 == None:
        print(f"ETL pipeline FAILED!")
    else:
        print(f"Entire ETL pipeline {status2}!")
    glue_client.close()

if __name__ == '__main__':
    run_etl_pipeline()