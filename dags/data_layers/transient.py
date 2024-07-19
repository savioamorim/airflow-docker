import json

from utils.utils import get_api_data, get_datetime_now, ingest_data

# with open("config/api_config.json", "r") as config_api:
#     config = json.load(config_api)

def process_transient():
    with open("config/api_config.json", "r") as config_api:
        config = json.load(config_api)

    url = config["endpoint"]
    query_params = config["params"]
    data_json = get_api_data(url, query_params)

    datetime_str, year, month, day = get_datetime_now()

    file_name = f"currency_{datetime_str}.json"
    path = f"/opt/airflow/dags/db/transient/{year}/{month}/{day}/"
    ingest_data(path, file_name, data_json)

#process_transient(config)