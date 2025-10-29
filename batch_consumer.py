import logging
import signal
import sys
import time
from kafka import KafkaConsumer
from message import MyMessage
from config import BOOTSTRAP_SERVERS, TOPIC

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("batch_consumer")

running = True

def shutdown(signum, frame):
    global running
    running = False

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        auto_offset_reset='earliest',
        enable_auto_commit=False,            # ручной коммит
        group_id='batch-consumer-group',
        value_deserializer=lambda b: b,
        consumer_timeout_ms=1000
    )

    logger.info("BatchMessageConsumer started")
    min_batch_size = 10
    batch = []

    try:
        while running:
            # getmany предпочтительнее для батчевого чтения
            records = consumer.poll(timeout_ms=1000, max_records=50)
            # records: {TopicPartition: [msg, ...], ...}
            for tp, msgs in records.items():
                for msg in msgs:
                    try:
                        payload = msg.value
                        try:
                            my_msg = MyMessage.from_json(payload)
                            batch.append((msg, my_msg))
                        except Exception as e:
                            logger.exception(f"Deserialization error offset {msg.offset}: {e}")
                            # не добавляем в batch
                    except Exception:
                        logger.exception("Error processing polled message, continuing")

            if len(batch) >= min_batch_size:
                # обрабатывать пачку
                try:
                    logger.info(f"Processing batch of size {len(batch)}")
                    for msg, my_msg in batch:
                        # пример обработки:
                        logger.info(f"Batch item: {my_msg}")
                        # при ошибке в обработке — логируем и продолжаем
                    # после успешной обработки всей пачки — один коммит
                    consumer.commit()  # sync commit
                    logger.info("Committed offsets for batch")
                    batch = []
                except Exception:
                    logger.exception("Error handling batch — will continue (no commit)")
                    # не коммитим, повторим при следующем poll (гарантия at-least-once)
            else:
                # если есть элементы, но мало, можно подождать немного или продолжить
                time.sleep(0.2)
    except Exception:
        logger.exception("Consumer loop ended with exception")
    finally:
        # при завершении — попробовать обработать оставшуюся пачку
        if batch:
            try:
                logger.info(f"Final processing of remaining batch size {len(batch)}")
                for msg, my_msg in batch:
                    logger.info(f"Batch item: {my_msg}")
                consumer.commit()
                logger.info("Committed offsets for final batch")
            except Exception:
                logger.exception("Failed to commit final batch")
        consumer.close()
        logger.info("BatchMessageConsumer closed")

if __name__ == "__main__":
    main()
