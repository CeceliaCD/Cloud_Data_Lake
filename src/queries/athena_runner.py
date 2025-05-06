import os
"""
abs_path_file = os.path.abspath(__file__)
curr_dir = os.path.dirname(abs_path_file)
parent_dir = os.path.dirname(curr_dir)
sys.path.insert(0, parent_dir)
"""
from utils.config_loader import load_json_config
LOCAL_CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'config', 'datalake_pipeline_config.json')
GLUE_CONFIG_PATH = '/tmp/config/datalake_pipeline_config.json'

config_path = GLUE_CONFIG_PATH if os.path.exists(GLUE_CONFIG_PATH) else LOCAL_CONFIG_PATH
config = load_json_config(config_path)

def load_queries_from_sql_file(file_path):
    with open(file_path, 'r') as qf:
        sql_content = qf.read()
    
    queries = [q.strip() for q in sql_content.splitlines() if q.strip()]
    return queries

def run_athena_queries(client, keypath, id):
    query_str = load_queries_from_sql_file('queries/create_tables.sql')
    for i, query in enumerate(query_str):
        athena_response_id = client.start_query_execution(
            QueryString=query,
            Query_Execution_Context={
                'Database': config["aws_athena"]["schema"],
                'Catalog': config["aws_athena"]["catalog"]
            },
            Result_Configuration={
                'OutputLocation': keypath,
                'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3'
                },
                'ExpectedBucketOwner': id,
                'AclConfiguration': {
                'S3AclOption': 'BUCKET_OWNER_FULL_CONTROL'
                }
            },
            ResultReuseConfiguration={
            'ResultReuseByAgeConfiguration': {
                'Enabled': True,
                'MaxAgeInMinutes': 10080
            }
        }
        )
    print(f"Submitted query {i+1}: ExecutionID {athena_response_id['QueryExecutionId']}")



