import os

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "my-topic")
