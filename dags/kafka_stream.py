import json
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'thuphan',
    'start_date': datetime(2024,1,1,1,00)    
}

def _get_data():
    res = requests.get("https://randomuser.me/api", timeout=999)
    res = res.json()['results'][0]
    print(json.dumps(res, indent=4))
    
    return res

def _format_data(res):
    data = {}
    data['first_name']

    return data

def _stream_data():
    pass

with DAG(
    'user_automation',
    default_args=default_args,
    catchup=False
) as dag:
    streaming = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=_stream_data
    )

_stream_data()
