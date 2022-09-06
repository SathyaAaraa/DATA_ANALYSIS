import pandas as pd

from imports import *

def load_json(json_file_path):
    f = open(json_file_path)
    json_data = json.load(f)
    return json_data
