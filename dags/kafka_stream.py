import json
from datetime import datetime
from urllib import response

import requests
from airflow import DAG
from airflow.decorators import dag, task
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
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}," \
        f"{location['city']}, {location['state']}, {location['country']}"
    
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    print(json.dumps(data, indent=4, ensure_ascii=False))
    print(data)
    return data

@dag(
    default_args=default_args,
    catchup=False
)
def user_automation():
    """DAG task flow definition"""

    @task
    def stream_data():
        print('Stream data ...')
        res = _get_data()
        _format_data(res)

    stream_data()

user_automation()
_format_data(_get_data())
