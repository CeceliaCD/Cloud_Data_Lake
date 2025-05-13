import pandas as pd
from io import StringIO
import csv
import json

#Setting up logging
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#obtain raw data from source
def extract_s3_data(client, keypath):
    raw_data_df = pd.DataFrame()
    bucketname = keypath.split('/')[2]
    key = keypath.split('/')[3] + '/'
    
    obj_list = client.list_objects_v2(Bucket=bucketname, Prefix=key)
    contents = obj_list.get('Contents', [])
    
    for obj in contents:
        key = obj["Key"]
        if 'pokemon' in keypath and keypath.endswith('.csv'):
            obj_response = client.get_object(Bucket=bucketname, Key=key)
            body = obj_response['Body'].read().decode('utf-8')
            reader = csv.DictReader(StringIO(body))
            raw_data = list(reader)
            raw_data_df = pd.concat([raw_data_df, pd.DataFrame(raw_data)], ignore_index=True)
        elif 'pokemon' in keypath and keypath.endswith('.json'):
            obj_response = client.get_object(Bucket=bucketname, Key=key)
            body = obj_response['Body'].read().decode('utf-8')
            raw_data_df = pd.concat([raw_data_df, pd.read_json(body)], ignore_index=True)
    print('File extracted from S3.')
    return raw_data_df