#!/bin/bash

set -e

echo "Waiting for Kafka..."

until docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --list >/dev/null 2>&1
do
    sleep 2
done

echo "Kafka is ready."

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic trading-events \
  --bootstrap-server kafka:29092 \
  --partitions 4 \
  --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic ema-results \
  --bootstrap-server kafka:29092 \
  --partitions 1 \
  --replication-factor 1

echo "Kafka topics ready:"
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --list