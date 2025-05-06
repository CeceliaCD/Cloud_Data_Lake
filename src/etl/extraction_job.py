import pandas as pd
#import requests
import csv
import json
import importlib.resources as pkg_resources
import config

config_path = pkg_resources.files(config).joinpath("datalake_pipeline_config.json")

with config_path.open('r', encoding='utf-8') as f:
    config_data = json.load(f)

#Setting up logging
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#obtain raw data from source
def extract_s3_data(client, keypath):
    raw_data_df = pd.DataFrame()
    if config_data['data_source']['URI'] == "":
        bucketname = config_data["s3_bucket"]["bucket"]
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