from importlib import resources

def load_queries_from_sql_file(file_path, filename):
    with resources.open_text(file_path, filename) as qf:
        sql_content = qf.read()
    queries = [q.strip() for q in sql_content.split(';') if q.strip()]
    return queries

def run_athena_queries(client, ikeypath, keypath, id):
    if ikeypath:
        query_str = load_queries_from_sql_file('pokemonetl.queries', 'create_tables.sql')
        for i, query in enumerate(query_str):
            athena_response_id = client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={
                    'Database': "pokemon_index_db",
                    'Catalog': "AwsDataCatalog"
                },
                ResultConfiguration={
                    'OutputLocation': keypath,
                    'EncryptionConfiguration': {
                        'EncryptionOption': 'SSE_S3'
                    },
                    'ExpectedBucketOwner': id,
                    'AclConfiguration': {
                        'S3AclOption': 'BUCKET_OWNER_FULL_CONTROL'
                    }
                }
            )
    print(f"Submitted query {i+1}: ExecutionID {athena_response_id['QueryExecutionId']}")



