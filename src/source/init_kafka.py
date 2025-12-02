from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable
import time
import subprocess

# Configuration - KRaft Mode (No Zookeeper)
KAFKA_BROKER = 'localhost:9092,localhost:9093,localhost:9094'

# Stream Topics Only
TOPICS = [
    'telecom-cdr',
    'telecom-sdr',
    'telecom-payments', 
    'telecom-location',
    'telecom-network',
    'telecom-security',
    'telecom-usage',
    'telecom-fraud-actions'
]

# Calculation for Partitions and Brokers:
# 1. Brokers: 3 brokers are selected for High Availability (HA). 
#    This allows for a Replication Factor of 3, ensuring data survives even if 2 brokers fail.
# 2. Partitions: Set to 3 (matching the number of brokers).
#    This ensures a balanced load where each broker acts as a leader for one partition per topic.
#    If throughput increases significantly, partitions can be increased to 6 or 9.

def check_kafka_containers():
    """Check if Kafka containers are running"""
    try:
        result = subprocess.run(
            ['docker', 'ps', '--filter', 'name=kafka', '--format', '{{.Names}}'],
            capture_output=True,
            text=True,
            check=True
        )
        containers = [line for line in result.stdout.strip().split('\n') if line]
        return containers
    except Exception as e:
        print(f"[WARN]  Could not check Docker containers: {e}")
        return []

def create_topics():
    # First, check if Kafka containers are running
    print("Checking Kafka containers...")
    kafka_containers = check_kafka_containers()
    
    if not kafka_containers:
        print("[ERROR] ERROR: No Kafka containers are running!")
        print("   Please run: docker-compose up -d")
        print("   Then wait 60 seconds and try again.")
        return
    
    print(f"[OK] Found {len(kafka_containers)} Kafka container(s):")
    for container in kafka_containers:
        print(f"   - {container}")
    
    max_retries = 12
    retry_delay = 10  # seconds
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"\nConnecting to Kafka Admin Client (Attempt {attempt}/{max_retries})...")
            admin_client = KafkaAdminClient(
                bootstrap_servers=KAFKA_BROKER,
                client_id='fraud-detection-admin',
                request_timeout_ms=15000,
                api_version_auto_timeout_ms=10000
            )
            
            # List existing topics
            existing_topics = admin_client.list_topics()
            print(f"[OK] Connected! Found {len(existing_topics)} existing topics.")
            
            # Create new topics
            topics_to_create = []
            for topic_name in TOPICS:
                if topic_name not in existing_topics:
                    topics_to_create.append(
                        NewTopic(
                            name=topic_name,
                            num_partitions=3,
                            replication_factor=3
                        )
                    )
            
            if topics_to_create:
                admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
                print(f"\n[SUCCESS] Created {len(topics_to_create)} topics successfully!")
                for topic in topics_to_create:
                    print(f"   [OK] {topic.name}")
            else:
                print("\n[SUCCESS] All topics already exist.")
                
            admin_client.close()
            return  # Success - exit function
            
        except NoBrokersAvailable:
            if attempt < max_retries:
                print(f"[WAIT] Kafka cluster not ready yet. Waiting {retry_delay} seconds...")
                print(f"   (This is normal - Kafka takes time to initialize)")
                time.sleep(retry_delay)
            else:
                print("\n[ERROR] Failed to connect to Kafka after all retries.")
                print("   Possible solutions:")
                print("   1. Increase wait time in startup.ps1 to 90 seconds")
                print("   2. Check logs: docker logs kafka-1")
                print("   3. Restart: docker-compose restart kafka-1 kafka-2 kafka-3")
        except Exception as e:
            print(f"[ERROR] Error: {e}")
            if attempt < max_retries:
                print(f"   Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("\n   Troubleshooting:")
                print("   - Check if ports 9092, 9093, 9094 are available")
                print("   - Run: docker-compose logs kafka-1")

if __name__ == "__main__":
    create_topics()
