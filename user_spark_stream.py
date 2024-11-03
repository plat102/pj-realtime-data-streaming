import logging

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col


def create_spark_connection():
    """create a spark session

    Spark Kafka connector is required
        https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
    Spark SQL Kafka connector is required
        https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
    """
    try:
        spark = (
            SparkSession.builder.appName("spark_user_streaming")
            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1",
            )
            .config("spark.cassandra.connection.host", "localhost")
            .getOrCreate()
        )
        return spark
    except Exception as e:
        logging.error("Error creating a Spark session: %s", e)
        return None


def create_cassandra_connection():
    """create a Cassandra connection"""
    try:
        # connect to the Cassandra cluster
        cluster = Cluster(contact_points=["localhost"])
        session = cluster.connect()
        auth_provider = PlainTextAuthProvider(
            username="cassandra", password="cassandra"
        )

        return session

    except Exception as e:
        logging.error("Error creating a Cassandra connection: %s", e)
        return None


# create a Cassandra keyspace and table
def create_keyspace(session):
    """create a keyspace for Spark streaming"""
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH REPLICATION = {
            'class' : 'SimpleStrategy',
            'replication_factor' : 1
        }                
    """)
    print("Keyspace created!")


def create_user_table(session):
    """create user table"""
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
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
    """)
    print("Table created!")


def insert_user_data(session, **kwargs):
    """insert user data into the table"""

    # extract the data
    user_uuid = kwargs.get("id")
    first_name = kwargs.get("first_name")
    last_name = kwargs.get("last_name")
    gender = kwargs.get("gender")
    address = kwargs.get("address")
    email = kwargs.get("email")
    postcode = kwargs.get("postcode")
    phone = kwargs.get("phone")
    cell = kwargs.get("cell")
    date_of_birth = kwargs.get("date_of_birth")
    age = kwargs.get("age")
    registered_date = kwargs.get("registered_date")
    picture = kwargs.get("picture")
    username = kwargs.get("username")

    try:
        session.execute(
            """
            INSERT INTO spark_streams.created_users (
                id, first_name, last_name, gender, address, email, 
                postcode, phone, cell, date_of_birth, age,
                registered_date, picture, username
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
            (
                user_uuid,
                first_name,
                last_name,
                gender,
                address,
                email,
                postcode,
                phone,
                cell,
                date_of_birth,
                age,
                registered_date,
                picture,
                username,
            ),
        )

        logging.info("Data inserted for %s %s", first_name, last_name)

    except Exception as e:
        logging.error("could not insert data due to %s", e)


if __name__ == "__main__":
    spark_connection = create_spark_connection()

    if spark_connection is not None:
        print("Spark connections are created!")
        cassandra_session = create_cassandra_connection()

        if cassandra_session is not None:
            print("Cassandra connection are created!")

            create_keyspace(cassandra_session)
            create_user_table(cassandra_session)
