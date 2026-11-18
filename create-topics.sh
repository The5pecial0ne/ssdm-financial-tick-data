#!/bin/bash
# Creates the Kafka topics the pipeline uses. Safe to run more than once.
#   ./create-topics.sh            create missing topics
#   ./create-topics.sh --reset    delete and recreate them (empties them before an experiment)
# PARTITIONS sets the input topic's partition count (default 8), which caps source parallelism.
set -e

PARTITIONS=${PARTITIONS:-8}
KT="docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092"

echo "Waiting for Kafka..."
until $KT --list >/dev/null 2>&1; do sleep 2; done
echo "Kafka is ready."

if [[ "$1" == "--reset" ]]; then
  for t in ticks ema-results advisories late-trades; do
    $KT --delete --if-exists --topic "$t" >/dev/null 2>&1 || true
  done
  sleep 3
fi

# ticks: one partition per possible source subtask; keyed by symbol so per-symbol order holds
$KT --create --if-not-exists --topic ticks --partitions "$PARTITIONS" --replication-factor 1
for t in ema-results advisories late-trades; do
  $KT --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1
done

echo "Kafka topics ready:"
$KT --list
