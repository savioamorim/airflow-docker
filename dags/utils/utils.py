import os
import json
import requests

from datetime import datetime

def get_datetime_now():
    date_today = datetime.now()
    datetime_str = date_today.strftime("%Y_%m_%d_%H_%M_%S")

    year = str(date_today.year)
    month = str(date_today.month).zfill(2)
    day = str(date_today.day).zfill(2)

    return datetime_str, year, month, day

def get_api_data(url, query_params):
    params = ",".join(query_params)
    result = requests.get(f"{url}/{params}")

    try:
        result_json = result.json()
    except:
        result_json = {}

    return result_json

def ingest_data(path, file_name, data):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

    file_path = os.path.join(path, file_name)

    if file_path.endswith(".json"):
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
    elif file_path.endswith(".parquet"):
        data.to_parquet(file_path, index=False)

def get_transient_bucket(path, file_name):
    with open(f"{path}/{file_name}", 'r') as file:
        data = json.load(file)

        print(data)

        print("")

        return data
