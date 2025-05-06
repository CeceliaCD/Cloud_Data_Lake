import pandas as pd
from io import BytesIO
import requests
import csv
import json
import pyarrow.parquet as pq
import avro.datafile
import avro.io
import importlib.resources as pkg_resources
import config

with pkg_resources.open_text(config, "datalake_pipeline_config.json") as f:
    config_file = json.load(f)

#Setting up logging
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#obtain raw data from source
def extract_s3_data(client, keypath):
    raw_data_df = pd.DataFrame()
    if config_file['data_source']['URI'] == "":
        bucketname = config_file["s3_bucket"]["bucket"]
        key = keypath.split('/')[3] + '/'
        
        obj_list = client.list_objects_v2(Bucket=bucketname, Prefix=key)
        contents = obj_list.get('Contents', [])
       
        for obj in contents:
            key = obj["Key"]
            if keypath.endswith('.csv'):
                obj_response = client.get_object(Bucket=bucketname, Key=key)
                f = obj_response['Body']
                raw_data = [row for row in csv.DictReader(f)]
                raw_data_df = pd.concat([raw_data_df, pd.DataFrame(raw_data)], ignore_index=True)
            if keypath.endswith('.json'):
                obj_response = client.get_object(Bucket=bucketname, Key=key)
                f = obj_response['Body']
                raw_data = json.load(f)
                raw_data_df = pd.concat([raw_data_df, pd.DataFrame(raw_data)], ignore_index=True)
            if keypath.endswith('.parquet'):
                obj_response = client.get_object(Bucket=bucketname, Key=key)
                file_stream = BytesIO(obj_response['Body'].read())
                table = pq.read_table(file_stream)
                raw_data_df = pd.concat([raw_data_df, table.to_pandas()], ignore_index=True)
            if keypath.endswith('.avro'):
                obj_response = client.get_object(Bucket=bucketname, Key=key)
                file_stream = BytesIO(obj_response['Body'].read())
                reader = avro.datafile.DataFileReader(file_stream, avro.io.DatumReader())
                records = [record for record in reader]
                reader.close()
                raw_data_df = pd.concat([raw_data_df, pd.DataFrame(records)], ignore_index=True)
    """
    else:
        data_response = requests.get(config['data_source']['URI'])
        if data_response.status_code == 200:
            content_type = data_response.headers.get('Content-Type')
            try:
                if "text" in content_type:
                    csv_data = data_response.content
                    raw_data = [row for row in csv.DictReader(csv_data)]
                    raw_data_df = pd.DataFrame(raw_data)
            except Exception as e:
                print(f'Data is not correct file format: {e}' )
    """  
    print('File extracted from S3.')
    return raw_data_df