#!/usr/bin/env python3
import os
import time
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

BOOTSTRAP = os.environ.get("BOOTSTRAP_SERVERS", "kafka-server:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "pages")

def create_topic():
    """Create Kafka topic with retry logic"""
    max_retries = 30
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect to Kafka at {BOOTSTRAP} (attempt {attempt + 1}/{max_retries})")
            admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
            
            # Create topic configuration
            topic = NewTopic(
                name=TOPIC,
                num_partitions=1,
                replication_factor=1
            )
            
            # Create the topic
            admin_client.create_topics([topic])
            print(f"Successfully created topic: {TOPIC}")
            admin_client.close()
            return
            
        except TopicAlreadyExistsError:
            print(f"Topic {TOPIC} already exists")
            return
        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                print(f"Kafka not available yet, retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Failed to connect to Kafka after all retries")
                raise
        except Exception as e:
            print(f"Unexpected error: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

if __name__ == "__main__":
    create_topic() 