from kafka import KafkaConsumer
import json
import time

topic = "my-topic"

consumer = KafkaConsumer(
    topic,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=False,  # ручной контроль коммитов
    group_id='batch-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    fetch_min_bytes=1024,         # 👈 добавлено: минимальный объем данных для fetch (1KB)
    fetch_max_wait_ms=500,        # 👈 добавлено: максимум 500мс ждать накопления batch
    max_poll_records=10
)

batch = []
batch_size = 5

try:
    print("Starting consumer...")
    while True:
        msg_pack = consumer.poll(timeout_ms=1000)
        for tp, messages in msg_pack.items():
            for message in messages:
                batch.append(message.value)

                if len(batch) >= batch_size:
                    print(f"Processing batch of {len(batch)} messages...")
                    try:
                        # Имитация обработки
                        for m in batch:
                            print(f"  -> {m}")
                        time.sleep(1)

                        consumer.commit()  # коммитим смещения после успешной обработки
                        print("Batch committed successfully!\n")
                        batch.clear()

                    except Exception as e:
                        print(f"Error during batch processing: {e}")
                        # Не коммитим, но batch не очищаем, чтобы не потерять сообщения
                        # Kafka повторит те же сообщения при следующем poll
                        print("Batch will be retried...\n")

except KeyboardInterrupt:
    print("\nGracefully shutting down...")
finally:
    if batch:
        print(f"Processing remaining {len(batch)} messages before exit...")
        try:
            for m in batch:
                print(f"  -> {m}")
            consumer.commit()
            print("Final batch committed.")
        except Exception as e:
            print(f"Error during final batch commit: {e}")
    consumer.close()
    print("Consumer closed.")
