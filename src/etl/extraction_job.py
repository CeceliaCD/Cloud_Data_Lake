import pandas as pd
from io import StringIO
import requests
from pathlib import Path
import os, sys
import csv

abs_path_file = os.path.abspath(__file__)
curr_dir = os.path.dirname(abs_path_file)
parent_dir = os.path.dirname(curr_dir)
sys.path.insert(0, parent_dir)
from utils.config_loader import load_json_config

config_file = load_json_config("config/datalake_pipeline_config.json")

#Setting up logging
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#obtain raw data from source
def extract_s3_data():
    if config_file['data_source']['URI'] == "":
        with open(config_file['data_source']['csv_path'], mode='r', encoding='utf-8') as f:
            raw_data = [row for row in csv.DictReader(f)]
    else:
        data_response = requests.get(config_file['data_source']['URI'])
        if data_response.status_code == 200:
            content_type = data_response.headers.get('Content-Type')
            try:
                if "text" in content_type:
                    csv_data = data_response.content
                    raw_data = [row for row in csv.DictReader(csv_data)]
            except Exception as e:
                print(f'Data is not correct file format: {e}' )
    return raw_data