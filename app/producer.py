# #!/usr/bin/env python3
import json
import os
import time
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


BOOTSTRAP = os.environ.get("BOOTSTRAP_SERVERS", "kafka-server:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "pages")
WIKIMEDIA_URL = os.environ.get("WIKIMEDIA_URL", "https://stream.wikimedia.org/v2/stream/page-create")

def create_producer():
    """Create Kafka producer with retry logic"""
    max_retries = 30
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect to Kafka at {BOOTSTRAP} (attempt {attempt + 1}/{max_retries})")
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("Successfully connected to Kafka!")
            return producer
        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                print(f"Kafka not available yet, retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Failed to connect to Kafka after all retries")
                raise
        except Exception as e:
            print(f"Unexpected error connecting to Kafka: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise
producer = create_producer()

response = requests.get(WIKIMEDIA_URL, stream=True)

print(f"Connected to Wikimedia stream at {WIKIMEDIA_URL}")
print(f"Producing to Kafka topic: {TOPIC}")

# Allowed domains
ALLOWED_DOMAINS = ["en.wikipedia.org", "www.wikidata.org", "commons.wikimedia.org"]

# Read stream line by line
for line in response.iter_lines(decode_unicode=True):
    if line and line.startswith('data: '):
        # Remove 'data: ' prefix and parse JSON
        json_data = line[6:]
        try:
            event = json.loads(json_data)

            # Get domain and performer info
            domain = event.get("meta", {}).get("domain")
            performer = event.get("performer", {})
            user_is_bot = performer.get("user_is_bot", False)
            if domain in ALLOWED_DOMAINS and not user_is_bot:
                # create message
                msg = {
                    "user_id": performer.get("user_id"),
                    "domain": domain,
                    "created_at": event.get("meta", {}).get("dt"),
                    "page_title": event.get("page_title")
                }

                # send to Kafka
                producer.send(TOPIC, msg)
                print(f"Sent message from {msg['domain']} - {msg['page_title']}")

        except json.JSONDecodeError:
            print("Invalid JSON data")
            continue