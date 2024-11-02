"""
DAG for automating user API data fetching
"""

import os
import json
from datetime import datetime

import requests
from dotenv import load_dotenv

# from airflow.decorators import dag, task
# from airflow.operators.python import PythonOperator

load_dotenv()
RANDOM_USER_API_URL = os.getenv("RANDOM_USER_API_URL")

default_args = {
    "owner": "thuphan",
    "start_date": datetime(2024, 11, 2, 10, 0),
}


# @dag(default_args=default_args, schedule_interval=None, catchup=False, tags=["example"])
def user_automation():
    """
    DAG for automating user API data fetching
    """

    def get_data():
        res = requests.get(RANDOM_USER_API_URL, timeout=5)

        results_data = res.json().get("results")[0]
        # print(json.dumps(results_data, indent=4))

        return results_data

    def format_user_data(response_data):
        """
        Format data from API
        """
        user_data = {}
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

        # print(json.dumps(user_data, indent=4))

    def stream_data():
        """
        Stream data to Kafka
        """
        pass

    res = get_data()
    format_user_data(res)


user_automation()
