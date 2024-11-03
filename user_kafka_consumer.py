import json
import logging
from kafka import KafkaConsumer
from utils.cassandra_functions import create_cassandra_session, insert_user_data

# Configure logging
logging.basicConfig(level=logging.INFO)

# Kafka configuration
KAFKA_TOPIC = 'users_created'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']


# Kafka consumer setup
def consume_from_kafka(session):
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='cassandra-consumer-group',
        value_deserializer=lambda x: x.decode('utf-8')
    )

    logging.info("Consuming messages from Kafka...")
    
    for message in consumer:
        try:
            # Parse JSON message
            user_data = json.loads(message.value)
            logging.info(f"Consumed message from Kafka: {user_data}")
            
            # Insert data into Cassandra
            insert_user_data(session, user_data)
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON message: {e}")
        except Exception as e:
            logging.error(f"Error processing message: {e}")

if __name__ == "__main__":
    # Create Cassandra session
    cassandra_session = create_cassandra_session()
    if cassandra_session:
        # Start consuming messages from Kafka and inserting into Cassandra
        consume_from_kafka(cassandra_session)
