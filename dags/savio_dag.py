import pandas as pd
import requests
import json

from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

def count_data_capture():
    url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    response = requests.get(url)
    df = pd.DataFrame(json.loads(response.content))
    qtd = len(df.index)
    return qtd

def is_valid(ti):
    qtd = ti.xcom_pull(task_ids="count_data_capture")
    if qtd > 10:
        return "verification_ok"
    return "verification_not_ok"

with DAG("savio_first_dag", 
         start_date = datetime(2024,1,1),
         schedule_interval = "30 * * * *",
         catchup = False) as dag:
    
    count_data_capture = PythonOperator(
        task_id = "count_data_capture",
        python_callable = count_data_capture
    )

    is_valid = BranchPythonOperator(
        task_id = "is_valid",
        python_callable = is_valid
    )

    verification_ok = BashOperator(
        task_id = "verification_ok",
        bash_command = "echo 'Quantity OK'"
    )

    verification_not_ok = BashOperator(
        task_id = "verification_not_ok",
        bash_command = "echo 'Quantity NOT OK'"
    )

    count_data_capture >> is_valid >> [verification_ok, verification_not_ok]
