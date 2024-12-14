import json
from hdfs import InsecureClient
from kafka import KafkaConsumer

# HDFS Client setup
hdfs_client = InsecureClient('http://localhost:9870', user='hadoop')

# Kafka Consumer setup
consumer = KafkaConsumer(
    'nike_reviews',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Define HDFS target path
hdfs_target_dir = "/users/roszhanraj/kafka_data/"

for message in consumer:
    try:
        record = message.value
        hdfs_file_path = f"{hdfs_target_dir}review_{record['id']}.json"
        with hdfs_client.write(hdfs_file_path, encoding="utf-8") as writer:
            writer.write(json.dumps(record, indent=4))
        print(f"File successfully written to HDFS: {hdfs_file_path}")
    except Exception as e:
        print(f"Error writing to HDFS: {e}")
