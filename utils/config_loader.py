import json
import os

def load_json_config(path):
    with open(path, "r") as f:
        config_file = json.load(f)
        return config_file