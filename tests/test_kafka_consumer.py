from kafka import KafkaConsumer

# Set up the consumer
consumer = KafkaConsumer(
    'users_created',            # Replace with your Kafka topic
    bootstrap_servers=['localhost:9092'],  # Replace with your broker address
    auto_offset_reset='earliest',          # Start reading at the beginning of the topic
    enable_auto_commit=True,               # Automatically commit offsets
    group_id='test-consumer-group',        # Consumer group ID
    value_deserializer=lambda x: x.decode('utf-8')  # Decode messages from bytes to string
)

print("Consuming messages from Kafka...")

try:
    # Consume messages
    for message in consumer:
        print(f"Key: {message.key}, Value: {message.value}, Offset: {message.offset}")
except KeyboardInterrupt:
    print("Stopped consuming.")
finally:
    consumer.close()
