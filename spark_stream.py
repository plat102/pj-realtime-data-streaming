"""
Entrypoint for Spark streaming into Cassandra
"""
import logging
import os

from cassandra.cluster import Cluster
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructField, StructType

# Load variables from .env file
load_dotenv()
# Get env variables values
cassandra_host = os.getenv('CASSANDRA_HOST')
cassandra_port = os.getenv('CASSANDRA_PORT')

# Connect Kafka using Spark (adapter layer)
def init_spark_session():
    """Init Spark session"""
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        # s_conn.sparkContext.setLogLevel("ERROR")
        print("Spark connection created successfully!")
    except Exception as e:
        print(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def connect_to_kafka(spark_conn):
    """Connect to Kafka using Spark"""
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df
    
def connect_to_cassandra():
    """Connect to Cassandra"""

    cassandra_cluster = Cluster(contact_points=[cassandra_host], port=cassandra_port)
    session = cassandra_cluster.connect()
    print('Connected to Cassandra.')
    return session

# Prepare target keyspaces and tables in Cassandra
def create_cassandra_keyspace(session):
    """Create target keyspace"""
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {
                'class': 'SimpleStrategy', 
                'replication_factor': '1'
            };
        """)
        print('Keyspace created successfully.')
    except Exception as e:
        logging.error(f'Error creating keyspace: {str(e)}')

def create_cassandra_table(session):
    """Create target table"""
    try:
        session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT
        );
        """)
        print('Table created successfully.')
    except Exception as e:
        print(f'Error creating table: {str(e)}')

# Extract data from Kafka (application layer)
def extract_df_from_kafka(spark_df):
    """Get data from kafka""" 
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

# Insert data into Cassandra
def insert_into_cassandra(session, **kwargs):
    """Insert to target table"""
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        print(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        print(f'could not insert data due to {e}')

# Run the streaming process (entrypoint)
if __name__ == '__main__':
    # Connect
    spark_session = init_spark_session()

    if spark_session is not None:
        spark_df = connect_to_kafka(spark_session)
        print(spark_df)
        # selection_df = extract_df_from_kafka(spark_df)
        # session = connect_to_cassandra()

        # if session is not None:

        #     create_cassandra_keyspace(session)
        #     create_cassandra_table(session)

        #     logging.info("Streaming is being started...")

        #     streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
        #                        .option('checkpointLocation', '/tmp/checkpoint')
        #                        .option('keyspace', 'spark_streams')
        #                        .option('table', 'created_users')
        #                        .start())

        #     streaming_query.awaitTermination()
