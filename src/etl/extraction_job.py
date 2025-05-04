import pandas as pd
from io import StringIO, BytesIO
import requests
from pathlib import Path
import os, sys
import csv
import json
import pyarrow.parquet as pq
import fastavro

abs_path_file = os.path.abspath(__file__)
curr_dir = os.path.dirname(abs_path_file)
parent_dir = os.path.dirname(curr_dir)
sys.path.insert(0, parent_dir)
from utils.config_loader import load_json_config

config = load_json_config("config/datalake_pipeline_config.json")

#Setting up logging
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#obtain raw data from source
def extract_s3_data(client, keypath):
    raw_data_df = pd.DataFrame()
    if config['data_source']['URI'] == "":
        bucketname = config["s3_bucket"]["bucket"]
        
        prefix_dir = keypath.split('/')[3] + '/'
        filename = os.path.basename(keypath)
        key = prefix_dir+filename
        
        obj_list = client.list_objects_v2(Bucket=bucketname, Prefix=key)
        contents = obj_list.get('Contents', [])
       
        for obj in contents:
            key = obj["Key"]
            if key.endswith('.csv'):
                obj_response = client.get_object(Bucket=bucketname, Key=key)
                f = obj_response['Body']
                raw_data = [row for row in csv.DictReader(f)]
                raw_data_df.append(raw_data)
            if key.endswith('.json'):
                obj_response = client.get_object(Bucket=bucketname, Key=key)
                f = obj_response['Body']
                raw_data = json.load(f)
                raw_data_df.append(raw_data)
            if key.endswith('.parquet'):
                obj_response = client.get_object(Bucket=bucketname, Key=key)
                file_stream = BytesIO(obj_response['Body'].read())
                table = pq.read_table(file_stream)
                raw_data_df.append(table.to_pandas())
            if key.endswith('.avro'):
                obj_response = client.get_object(Bucket=bucketname, Key=key)
                file_stream = BytesIO(['Body'].read())
                reader = fastavro.reader(file_stream)
                records = [record for record in reader]
                raw_data_df.append(records)
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
    return raw_data_df