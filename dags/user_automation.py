"""
DAG docs: DAG for automating user API data fetching
"""

import os
import time
import json
import uuid
from datetime import datetime

import requests
from dotenv import load_dotenv

from airflow.decorators import dag, task
from kafka import KafkaProducer

load_dotenv()
RANDOM_USER_API_URL = os.getenv("RANDOM_USER_API_URL")
# RANDOM_USER_API_URL = "https://randomuser.me/api/"

default_args = {
    "owner": "thuphan",
    "start_date": datetime(2024, 11, 2, 10, 0),
}


@dag(default_args=default_args, schedule_interval=None, catchup=False, tags=["example"])
def user_automation():
    """
    DAG for automating user API data fetching
    """

    def get_data():
        """
        Fetch user data from API
        """
        res = requests.get(RANDOM_USER_API_URL, timeout=5)
        results_data = res.json().get("results")[0]
        # Output raw user data for verification
        # print("Raw: ", json.dumps(results_data, indent=4))
        return results_data

    def format_user_data(response_data):
        """
        Format data from API
        """
        user_data = {}
        user_data['id'] = str(uuid.uuid4())
        user_data["first_name"] = response_data["name"]["first"]
        user_data["last_name"] = response_data["name"]["last"]
        user_data["gender"] = response_data["gender"]
        user_data["email"] = response_data["email"]
        user_data["address"] = (
            f"{response_data['location']['street']['number']} {response_data['location']['street']['name']}, "
            f"{response_data['location']['city']}, {response_data['location']['state']}, "
            f"{response_data['location']['country']}"
        )
        user_data["postcode"] = response_data["location"]["postcode"]
        user_data["phone"] = response_data.get("phone", None)
        user_data["cell"] = response_data["cell"]
        user_data["date_of_birth"] = response_data["dob"]["date"]
        user_data["age"] = response_data["dob"]["age"]
        user_data["registered_date"] = response_data["registered"]["date"]
        user_data["picture"] = response_data["picture"]["medium"]
        user_data["username"] = response_data["login"]["username"]

        # Output formatted user data for verification
        print("Formatted: ", json.dumps(user_data, indent=4))

        return user_data

    @task
    def stream_data_from_api():
        """
        Stream data to Kafka
        """

        current_time = time.time()

        # Connect to Kafka and send data to topic
        producer = KafkaProducer(
            bootstrap_servers=["broker:29092"], max_block_ms=5000
        )
        
        while True:
            # Fetch and send data to Kafka in 60 seconds
            if time.time() > current_time + 10:
                break
            
            try:
                response_data = get_data()
                user_data = format_user_data(response_data)
        
                producer.send("users_created", json.dumps(user_data).encode("utf-8"))
                producer.flush()  # Ensure message is sent
                print("Message sent to Kafka:", user_data)
            except Exception as e:
                print(f"Failed to send message to Kafka: {e}")

    stream_data_from_api()


user_automation()
