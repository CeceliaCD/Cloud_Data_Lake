import pandas as pd
import os
import csv
import json

#Setting up logging
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#obtain raw data from source
def extract_s3_data(client, keypath):
    raw_data_df = pd.DataFrame()
    bucketname = os.path.dirname(os.path.dirname(keypath))
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
    print('File extracted from S3.')
    return raw_data_df