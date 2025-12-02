from kafka import KafkaConsumer
import json
import time
import os

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092,localhost:9093,localhost:9094')
TOPIC_NAME = 'telecom-fraud-actions'

def block_user(user_id, reason):
    """
    Simulate a blocking API call.
    In production, this would make an HTTP request to the HLR/HSS or Policy Server.
    """
    # Simulate API latency
    time.sleep(0.05) 
    print(f"[ACTION] 🚫 BLOCKED User {user_id} | Reason: {reason}")

def consume_actions():
    print(f"Starting Action Consumer on topic: {TOPIC_NAME}")
    print("Waiting for fraud alerts...")
    
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='fraud-action-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    try:
        for message in consumer:
            alert = message.value
            
            # Extract details
            user_id = alert.get('caller_number') or alert.get('customer_id')
            reason = alert.get('alert_type', 'Unknown Fraud')
            
            if user_id:
                block_user(user_id, reason)
            else:
                print(f"[WARN] Received alert without user ID: {alert}")
                
    except KeyboardInterrupt:
        print("Stopping consumer...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_actions()
