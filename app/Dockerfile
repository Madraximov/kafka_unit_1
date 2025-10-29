FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "producer.py"]
# Примечание: чтобы запускать разные ролики (producer/consumer),
# в docker-compose либо в run-команде можно переопределять команду:
# docker-compose run --rm app python single_consumer.py
FROM ubuntu:latest
LABEL authors="amadrakhimov"

ENTRYPOINT ["top", "-b"]