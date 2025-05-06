import pandas as pd
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

#Setting up logging
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def load_s3_data(client, df, keypath):
    bucketname = config["s3_bucket"]["bucket"]
    try:    
        #Transformed raw data to processed df and uploading to processed/
        if isinstance(df, pd.DataFrame):
            key = keypath.split('/')[3]+'/'
            result_file = df.to_json("pokemon_processsed_data_results.json", orient='records')  #index
            #df.to_parquet('result_file, engine='pyarrow', index=False)  
            client.upload_file(result_file, bucketname, key)
        else:
            #Uploading the raw data file to raw/
            client.upload_file(df, bucketname, keypath)
        return f"File uploaded successfully to {bucketname}/{keypath}."
    except Exception as e:
        print(f"File upload failed: {e}")
    