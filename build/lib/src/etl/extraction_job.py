import pandas as pd
from io import BytesIO
import requests
import os
import csv
import json
import pyarrow.parquet as pq
import avro.datafile
import avro.io
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

#Setting up logging
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#obtain raw data from source
def extract_s3_data(client, keypath):
    raw_data_df = pd.DataFrame()
    if config['data_source']['URI'] == "":
        bucketname = config["s3_bucket"]["bucket"]
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