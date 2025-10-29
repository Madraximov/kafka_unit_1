import time
import logging
import random
import signal
import sys
from kafka import KafkaProducer
from message import MyMessage
from config import BOOTSTRAP_SERVERS, TOPIC

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("producer")

# Producer settings for At-Least-Once:
# - acks='all' ensures leader waits for ISR (in-sync replicas) -> stronger durability
# - retries>0 allows producer to retry on transient errors
# - linger_ms / batch_size can be tuned for throughput
producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    acks='all',            # wait for all in-sync replicas -> durability
    retries=5,             # retry sends on failure
    value_serializer=lambda v: v if isinstance(v, bytes) else v.encode('utf-8'),
    linger_ms=5,
)

running = True

def shutdown(signum, frame):
    global running
    logger.info("Shutting down producer...")
    running = False

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

def on_send_success(record_metadata):
    logger.info(f"Message delivered to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")

def on_send_error(excp):
    logger.error("Failed to deliver message", exc_info=excp)
    # at-least-once: failure logged; retries configured above may retry

def main():
    i = 0
    while running:
        try:
            msg = MyMessage(message_id=i, payload=f"hello {i}", ts=time.strftime("%Y-%m-%d %H:%M:%S"))
            payload_bytes = msg.to_json()
            logger.info(f"Sending: {payload_bytes.decode('utf-8')}")
            # send асинхронно с колбэками
            future = producer.send(TOPIC, value=payload_bytes)
            future.add_callback(on_send_success)
            future.add_errback(on_send_error)
            i += 1
            time.sleep(0.5)  # пауза между сообщениями
        except Exception as e:
            logger.exception("Exception in producer loop")
            time.sleep(1)

    # при завершении — подождём отправки
    producer.flush(timeout=10)
    producer.close()
    logger.info("Producer closed")

if __name__ == "__main__":
    main()
