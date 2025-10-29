import logging
import signal
import sys
from kafka import KafkaConsumer
from message import MyMessage
from config import BOOTSTRAP_SERVERS, TOPIC

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("single_consumer")

running = True

signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))

def main():
    # Этот консьюмер читает по одному сообщению и использует авто-коммит
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        auto_offset_reset='earliest',
        enable_auto_commit=True,          # автоматический коммит
        group_id='single-consumer-group', # уникальный group_id для параллельного чтения
        value_deserializer=lambda b: b,   # десериализуем вручную ниже
        consumer_timeout_ms=1000
    )

    logger.info("SingleMessageConsumer started")
    try:
        for msg in consumer:
            try:
                # msg.value — bytes
                payload = msg.value
                try:
                    my_msg = MyMessage.from_json(payload)
                    logger.info(f"Received (single): {my_msg}")
                    # обработка — имитация
                    # (если ошибка — отловим и продолжим)
                except Exception as e:
                    logger.exception(f"Deserialization error for offset {msg.offset}: {e}")
                    # continue: по заданию вывести ошибку и продолжать
                    continue

                # auto commit happens by broker/client periodically
            except Exception as e:
                logger.exception("Error handling message, continuing")
    except Exception as e:
        logger.exception("Consumer stopped because of exception")
    finally:
        consumer.close()
        logger.info("SingleMessageConsumer closed")

if __name__ == "__main__":
    main()
