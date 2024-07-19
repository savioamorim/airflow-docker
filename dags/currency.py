from data_layers.transient import process_transient
from data_layers.raw import process_raw
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

with DAG("currency_dag", 
         start_date = datetime(2024,1,1),
         schedule_interval = "0 18 * * 1-5",
         catchup = False) as dag:
    
    transient = PythonOperator(
        task_id = "transient_process",
        python_callable = process_transient
    )

    raw = PythonOperator(
        task_id = "raw_process",
        python_callable = process_raw
    )

    transient >> raw
