import pandas as pd
import os, sys
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

def load_s3_data(client, df, keypath):
    bucketname = config["s3_bucket"]["bucket"]
    try:
        prefix_dir = keypath.split('/')[3] + '/'
        filename = os.path.basename(keypath)
        key = prefix_dir+filename
        result_file = 'sample_data/' + filename
        if isinstance(df, pd.DataFrame):
            df.to_json(result_file, orient='records')  #index
            #df.to_parquet('result_file, engine='pyarrow', index=False)  
            client.upload_file(result_file, bucketname, key)
        else:
            client.upload_file(df, bucketname, key)
        return f"File {filename} uploaded successfully to {bucketname}/{keypath}."
    except Exception as e:
        print(f"File upload failed: {e}")
    