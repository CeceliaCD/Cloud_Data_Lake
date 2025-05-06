import json
import importlib.resources as pkg_resources
import config

config_path = pkg_resources.files(config).joinpath("datalake_pipeline_config.json")

with config_path.open('r', encoding='utf-8') as f:
    config_data = json.load(f)

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
                'Database': config_data["aws_athena"]["schema"],
                'Catalog': config_data["aws_athena"]["catalog"]
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



