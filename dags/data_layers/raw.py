import json
import pandas as pd

from utils.utils import get_transient_bucket, ingest_data, get_datetime_now

def process_raw():
    data = get_transient_bucket("/opt/airflow/dags/db/transient/2024/07/19", "currency_2024_07_19_13_15_45.json")

    df = pd.DataFrame(data).T.reset_index()
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

    path = f"/opt/airflow/dags/db/raw/{year}/{month}/{day}/"
    file_name = f"currency_{datetime_str}.parquet"
    ingest_data(path, file_name, df)

#process_raw(config)