"""Cassandra utilities for inserting data into a Cassandra table.
"""
import logging
import uuid
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


# Configure logging
logging.basicConfig(level=logging.INFO)


# Cassandra configuration
CASSANDRA_HOST = 'localhost'
CASSANDRA_KEYSPACE = 'spark_streams'
CASSANDRA_TABLE = 'created_users'

# Cassandra setup
def create_cassandra_session():
    try:
        auth_provider = PlainTextAuthProvider(username="cassandra", password="cassandra")
        cluster = Cluster(contact_points=[CASSANDRA_HOST], auth_provider=auth_provider)
        session = cluster.connect()
        
        # Create keyspace and table if they don't exist
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
            WITH REPLICATION = {{
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }}
        """)
        
        session.set_keyspace(CASSANDRA_KEYSPACE)
        
        session.execute(f"""
            CREATE TABLE IF NOT EXISTS {CASSANDRA_TABLE} (
                id UUID PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                gender TEXT,
                address TEXT,
                email TEXT,
                postcode TEXT,
                phone TEXT,
                cell TEXT,
                date_of_birth TEXT,
                age INT,
                registered_date TEXT,
                picture TEXT,
                username TEXT
            )
        """)
        
        return session
    except Exception as e:
        logging.error(f"Error creating Cassandra session: {e}")
        return None

# Insert data into Cassandra
def insert_user_data(session, user_data):
    try:
        session.execute(
            f"""
            INSERT INTO {CASSANDRA_TABLE} (
                id, first_name, last_name, gender, address, email,
                postcode, phone, cell, date_of_birth, age,
                registered_date, picture, username
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                uuid.uuid4(),  # Generate a unique ID
                user_data.get("first_name"),
                user_data.get("last_name"),
                user_data.get("gender"),
                user_data.get("address"),
                user_data.get("email"),
                str(user_data.get("postcode")),
                user_data.get("phone"),
                user_data.get("cell"),
                user_data.get("date_of_birth"),
                user_data.get("age"),
                user_data.get("registered_date"),
                user_data.get("picture"),
                user_data.get("username")
            )
        )
        logging.info(f"Inserted data for user {user_data.get('first_name')} {user_data.get('last_name')}")
    except Exception as e:
        logging.error(f"Could not insert data into Cassandra: {e}")
        