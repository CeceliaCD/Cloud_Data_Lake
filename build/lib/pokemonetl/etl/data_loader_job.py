import pandas as pd
import os

#Setting up logging
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def load_s3_data(client, df, keypath):
    bucketname = keypath.split('/')[2]
    try:    
        #Transformed raw data to processed df and uploading to processed/
        if isinstance(df, pd.DataFrame):
            filename = "pokemon_processsed_data_results.json"
            key = keypath.split('/')[3]+'/'
            df.to_json(filename, orient='records')  #index
            #df.to_parquet('result_file, engine='pyarrow', index=False)  
            client.upload_file(filename, bucketname, key+filename)
        else:
            #Uploading the raw data file to raw/
            client.upload_file(df, bucketname, keypath)
        return f"File uploaded successfully to {bucketname}/{keypath}."
    except Exception as e:
        print(f"File upload failed: {e}")
    