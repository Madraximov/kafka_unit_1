from kafka import KafkaProducer
import json
import signal
import sys
import time
import uuid

# Создание продюсера
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=3
)

topic = "my-topic"

# Graceful shutdown
def shutdown(signum, frame):
    print("\nFlushing pending messages and closing producer...")
    producer.flush()
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def delivery_report(record_metadata):
    print(f"Message delivered to {record_metadata.topic} "
          f"partition {record_metadata.partition} "
          f"offset {record_metadata.offset}")


def send_message():
    while True:
        message_id = str(uuid.uuid4())
        message = {"message_id": message_id, "value": f"Hello Kafka {message_id}"}

        # используем message_id для определения партиции (по hash)
        partition_key = message_id.encode('utf-8')

        future = producer.send(
            topic=topic,
            value=message,
            key=partition_key  # 👈 добавили ключ для распределения по партициям
        )
        future.add_callback(delivery_report)

        time.sleep(1)


if __name__ == "__main__":
    print("Starting producer...")
    send_message()
