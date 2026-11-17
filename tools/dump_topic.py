"""
Copies a Kafka topic into a JSON-lines file, adding each record's Kafka timestamp
as "kafkaTs" (when Flink wrote it). Stops once the topic has been quiet for --idle seconds.

    python -m tools.dump_topic ema-results results/week-flink.jsonl
"""
from __future__ import annotations

import argparse
import json
import time
import uuid

from confluent_kafka import Consumer, KafkaError


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("topic")
    ap.add_argument("output")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--idle", type=float, default=15, help="stop after this many seconds without new records")
    ap.add_argument("--wait-first", type=float, default=60, help="how long to wait for the first record")
    ap.add_argument("--raw", action="store_true", help="records aren't JSON; write {'value': ..., 'kafkaTs': ...}")
    a = ap.parse_args()

    c = Consumer({"bootstrap.servers": a.bootstrap, "group.id": f"dump-{uuid.uuid4()}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([a.topic])
    n, last, started = 0, None, time.time()
    with open(a.output, "w", encoding="utf-8") as f:
        # Joining the consumer group takes a few seconds, so the idle clock only starts at the first record
        while (time.time() - last < a.idle) if last else (time.time() - started < a.wait_first):
            msgs = c.consume(10000, timeout=1.0)
            for m in msgs:
                if m.error():
                    if m.error().code() != KafkaError._PARTITION_EOF:
                        print("error:", m.error())
                    continue
                ts = m.timestamp()[1]
                val = m.value().decode("utf-8")
                rec = {"value": val} if a.raw else json.loads(val)
                rec["kafkaTs"] = ts
                f.write(json.dumps(rec) + "\n")
                n += 1
            if msgs:
                last = time.time()
    c.close()
    print(f"wrote {n:,} records from {a.topic} to {a.output}")


if __name__ == "__main__":
    main()
