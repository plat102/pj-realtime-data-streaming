import json
import logging
import time
from datetime import datetime

import requests
from airflow.decorators import dag, task
from kafka import KafkaProducer

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

    return data

@dag(
    default_args=default_args,
    catchup=False
)
def user_automation():
    """DAG task flow definition"""

    @task
    def stream_data():

        # Send message
        producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
        curr_time = time.time()

        while True:
            if time.time() > curr_time + 60:
                break
            try:
                # Get data
                res = _get_data()
                res = _format_data(res)

                producer.send(topic='users_created', value=json.dumps(res).encode('utf-8'))
            except Exception as e:
                logging.error(f'Error occur: {e}')
                continue

    stream_data()

user_automation()
    