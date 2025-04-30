import sys, os
#sys.path.append('Cloud_Data_Lake/src/etl')
#import extraction_job
import pandas as pd
import src.etl.extraction_job
import src.etl.transformer_job 
import src.etl.data_load_job
import src.queries.athena_runner
import datetime as datetime

import logging
logger = logging.getLogger(__name__)

def main():
    rawdata = src.etl.extraction_job.extract_s3_data()
    print(rawdata)
    #print(rawdata_df.to_string())
    raw_data_df, cleaned_data_df = src.etl.transformer_job.transform_data(rawdata)
    #curated_data_df = {}
    #curated_data_df = src.queries.athena_runner.
    #src.etl.data_load_job.load_s3_data(raw_data_df, cleaned_data_df, curated_data_df)
    #src.etl.data_load_job.load_catalog_data(curated_data_df)

if __name__ == '__main__':
    main()