#!/usr/bin/env python3
"""
Copies everything currently in one or more Kafka topics into JSON Lines files.

Each line is the message as the Flink job wrote it, plus two extra fields:
    _kafka_ts   when the broker received the message (ms)
    _topic      the topic it came from

It reads from the start of the topic up to where the topic ended when the
script started, then stops, so it also works while a job is still running.

    python tools/collect.py --topics ema-results,advisories --out experiments/results/raw
"""

import argparse
import json
import os
import sys

from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING


def collect(bootstrap, topic, out_path, run_id=None):
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "collect-" + topic,
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    meta = consumer.list_topics(topic, timeout=30).topics[topic]
    if meta.error is not None:
        sys.exit(f"Topic {topic} is not available: {meta.error}")

    # Remember where each partition ends right now; that is our finish line.
    remaining, assignment = {}, []
    for partition in meta.partitions:
        tp = TopicPartition(topic, partition)
        low, high = consumer.get_watermark_offsets(tp, timeout=30)
        if high > low:
            remaining[partition] = high
            assignment.append(TopicPartition(topic, partition, OFFSET_BEGINNING))

    written = 0
    with open(out_path, "w", encoding="utf-8", newline="\n") as out:
        if assignment:
            consumer.assign(assignment)
        quiet_polls = 0
        while remaining and quiet_polls < 6:
            batch = consumer.consume(num_messages=5000, timeout=5.0)
            quiet_polls = 0 if batch else quiet_polls + 1
            for msg in batch:
                if msg.error():
                    sys.exit(f"Kafka error while reading {topic}: {msg.error()}")
                partition = msg.partition()
                if partition in remaining and msg.offset() < remaining[partition]:
                    record = json.loads(msg.value())
                    if run_id is None or record.get("run_id") == run_id:
                        record["_kafka_ts"] = msg.timestamp()[1]
                        record["_topic"] = topic
                        out.write(json.dumps(record) + "\n")
                        written += 1
                if partition in remaining and msg.offset() >= remaining[partition] - 1:
                    del remaining[partition]
    consumer.close()
    if remaining:
        print(f"WARNING: stopped waiting for {topic}, partitions {sorted(remaining)} did not reach their end")
    return written


def main():
    p = argparse.ArgumentParser(description="Dump Kafka topics to JSON Lines.")
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--topics", default="ema-results,advisories,late-trades")
    p.add_argument("--out", required=True, help="folder for <topic>.jsonl files")
    p.add_argument("--run-id", help="keep only lines with this run_id")
    args = p.parse_args()

    os.makedirs(args.out, exist_ok=True)
    for topic in [t.strip() for t in args.topics.split(",") if t.strip()]:
        path = os.path.join(args.out, topic + ".jsonl")
        print(f"{topic}: {collect(args.bootstrap, topic, path, args.run_id):,} lines -> {path}", flush=True)


if __name__ == "__main__":
    main()
