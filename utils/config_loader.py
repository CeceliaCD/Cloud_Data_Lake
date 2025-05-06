import json
import os

def load_json_config(path):
    if not os.path.isabs(path):
        possible_path = os.path.join('/tmp', path)
        if os.path.exists(possible_path):
            path = possible_path
    with open(path, "r") as f:
        config_file = json.load(f)
        return config_file