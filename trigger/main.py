import os
from src.etl.load import data_s3_upload_job
from src.etl.extract import api_extraction_job
from src.etl.transform import transformer_job
import datetime as datetime

def main():
    api_rawdata_df = api_extraction_job()
    cleaned_data_df = transformer_job(api_rawdata_df)
    data_s3_upload_job()

if __name__ == '__main__':
    main()