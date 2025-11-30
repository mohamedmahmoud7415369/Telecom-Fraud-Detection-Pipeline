import time
import json
import pandas as pd
import os
from kafka import KafkaProducer

# Configuration
KAFKA_BROKER = 'localhost:9092,localhost:9093,localhost:9094'
# Handle Path for Windows vs WSL
if os.path.exists(r'D:\ITI-Data_Engineer\Projects\Final Project\Data\Source_System_Data'):
    DATA_DIR = r'D:\ITI-Data_Engineer\Projects\Final Project\Data\Source_System_Data'
else:
    # WSL Path
    DATA_DIR = '/mnt/d/ITI-Data_Engineer/Projects/Final Project/Data/Source_System_Data'

TOPIC_MAP = {
    'cdr_data.csv': 'telecom-cdr',
    'complaints_feedback.csv': 'telecom-complaints',
    'customer_behavior.csv': 'telecom-behavior',
    'customer_profiles.csv': 'telecom-profiles',
    'device_information.csv': 'telecom-device',
    'location_updates.csv': 'telecom-location',
    'network_metrics.csv': 'telecom-network',
    'payment_transactions.csv': 'telecom-payments',
    'sdr_data.csv': 'telecom-sdr',
    'security_events.csv': 'telecom-security',
    'service_usage.csv': 'telecom-usage'
}

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"Connected to Kafka at {KAFKA_BROKER}")
        return producer
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return None

def stream_csv_to_kafka(producer, file_name, topic):
    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    print(f"Streaming {file_name} to topic {topic}...")
    
    try:
        # Read CSV in chunks to simulate streaming and handle large files
        for chunk in pd.read_csv(file_path, chunksize=1000):
            for _, row in chunk.iterrows():
                message = row.to_dict()
                producer.send(topic, value=message)
            
            producer.flush()
            time.sleep(0.1) # Brief delay (100ms per 1000 records)
            print(f"Sent batch of {len(chunk)} records to {topic}")
            
    except Exception as e:
        print(f"Error processing {file_name}: {e}")

def main():
    producer = create_producer()
    if not producer:
        return

    # Stream all files sequentially
    for file_name, topic in TOPIC_MAP.items():
        stream_csv_to_kafka(producer, file_name, topic)

if __name__ == "__main__":
    main()
