import pandas as pd

from utils.utils import ingest_data, get_datetime_now

def process_raw(ti):
    transient_data = ti.xcom_pull(task_ids="transient_process")

    df = pd.DataFrame(transient_data).T.reset_index()
    df = df.rename(columns={'index': 'ds_currency_pair',
                            'code': 'ds_code',
                            'codein': 'ds_codein',
                            'name': 'ds_name',
                            'high': 'vl_high',
                            'low': 'vl_low',
                            'varBid': 'vl_var_bid',
                            'pctChange': 'vl_pct_change',
                            'bid': 'vl_bid',
                            'ask': 'vl_ask',
                            'timestamp': 'nu_timestamp',
                            'create_date': 'dt_created_at'})
    
    datetime_str, year, month, day = get_datetime_now()

    if not df.empty:
        path = f"/opt/airflow/dags/db/raw/{year}/{month}/{day}/"
        file_name = f"currency_{datetime_str}.parquet"
        ingest_data(path, file_name, df)
