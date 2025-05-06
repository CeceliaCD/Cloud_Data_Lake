import pandas as pd
import json
import importlib.resources as pkg_resources
import config

with pkg_resources.open_text(config, "datalake_pipeline_config.json") as f:
    config_file = json.load(f)

#Setting up logging
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def load_s3_data(client, df, keypath):
    bucketname = config_file["s3_bucket"]["bucket"]
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
    