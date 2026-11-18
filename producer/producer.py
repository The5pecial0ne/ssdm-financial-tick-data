"""
Replays DEBS 2022 tick data into Kafka.

Each event becomes one compact CSV record, ID,SecType,Date,Time,TradingTime,Last,seq,
keyed by symbol. The key matters: Kafka keeps order per partition, so every trade of one
symbol lands in the same partition and reaches Flink in file order.

Examples, from the repo root:
    # the small test slice, as fast as possible
    python producer/producer.py --file analysis/test-slice.csv
    # all trades of the working week, as fast as possible (fills the topic for experiments)
    python producer/producer.py --days weekdays
    # Monday's busy morning, every event type, at 50,000 events per second
    python producer/producer.py --days 08-11-21 --events all --from 08:00 --to 12:00 --rate 50000
    # replay 1 hour of market time per minute (60x real time)
    python producer/producer.py --days 08-11-21 --speed 60

When it finishes it prints its throughput and, with --stats, writes it as JSON. With
--boundaries it also logs, for every 5-minute window, the wall-clock time the first event of the
next window was sent. That's the moment a window becomes complete (spec 3.1, item 7), and the
latency analysis measures from there to the advisory coming out of Flink.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tools import ticks  # noqa: E402

try:
    from confluent_kafka import Producer
except ImportError:
    sys.exit("confluent-kafka is missing: pip install -r requirements.txt")


def make_producer(bootstrap: str) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap,
        "linger.ms": 20,
        "batch.num.messages": 10000,
        "queue.buffering.max.messages": 1_000_000,
        "queue.buffering.max.kbytes": 1_048_576,
        "compression.type": "lz4",
        "acks": "1",
    })


def send(p: Producer, topic: str, key: bytes, value: bytes):
    while True:
        try:
            p.produce(topic, value=value, key=key)
            return
        except BufferError:
            p.poll(0.05)   # local queue full: let librdkafka drain it, then retry


def announce_start(p: Producer, topic: str, ts_ms: int, pause: float):
    """Sends a start marker to every partition, then gives Flink a moment before the data follows.

    A job that is already running marks its Kafka partitions idle while no input arrives (--idle-ms).
    When the replay starts, the first partition to receive trades would then drive the watermark on
    its own, and trades landing in the other partitions a few milliseconds later would already count
    as late. The marker wakes every partition up first, so they hold the watermark back together.
    """
    marker = f"#SOS,{ts_ms}".encode()
    for part in p.list_topics(topic, timeout=10).topics[topic].partitions:
        p.produce(topic, value=marker, partition=part)
    p.flush(30)
    time.sleep(pause)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="ticks")
    ap.add_argument("--file", action="append", help="replay these CSV files (in order) instead of --days")
    ap.add_argument("--days", default="weekdays", help="week, weekdays, or e.g. 08-11-21,09-11-21")
    ap.add_argument("--data-dir", default=str(ticks.DATA_DIR))
    ap.add_argument("--events", choices=["trades", "all"], default="trades",
                    help="trades = events with a Last price; all = also quotes and housekeeping events")
    ap.add_argument("--exchanges", help="only these exchanges, e.g. FR or FR,NL")
    ap.add_argument("--from", dest="t_from", help="HH:MM, only events from this time of day")
    ap.add_argument("--to", dest="t_to", help="HH:MM, only events before this time of day")
    ap.add_argument("--rate", type=float, default=0, help="cap in events per second (0 = as fast as possible)")
    ap.add_argument("--speed", type=float, default=0, help="replay at N times market speed (0 = off)")
    ap.add_argument("--limit", type=int, default=0, help="stop after this many events")
    ap.add_argument("--no-sos", action="store_true", help="don't send the start-of-stream marker")
    ap.add_argument("--start-pause", type=float, default=2.0,
                    help="seconds to wait after the start marker (keep it under the job's --idle-ms)")
    ap.add_argument("--no-eos", action="store_true",
                    help="don't send the end-of-stream marker that flushes the last windows")
    ap.add_argument("--boundaries", help="CSV log of when each window boundary was crossed")
    ap.add_argument("--stats", help="write throughput numbers to this JSON file")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()

    paths = [Path(f) for f in a.file] if a.file else [ticks.day_file(d, a.data_dir) for d in ticks.resolve_days(a.days)]
    missing = [str(p) for p in paths if not p.exists()]
    if missing:
        sys.exit("missing data file(s): " + ", ".join(missing) + "\nSee README, section 'Get the data'.")
    exchanges = a.exchanges.split(",") if a.exchanges else None

    p = make_producer(a.bootstrap)
    topic = a.topic
    sent = 0
    t_start = time.time()
    first_ts = None            # event time of the first trade, for --speed
    max_window = -1            # highest 5-minute window index seen so far
    boundaries = []            # (window_end_ms, wall_ms)
    last_report = t_start
    started = False

    for day_index, path in enumerate(paths):
        if not a.quiet:
            print(f"replaying {path.name} ...", flush=True)
        for chunk in ticks.read_chunks(path, seq_offset=day_index * 10**10):
            chunk = ticks.apply_filters(chunk, a.events, exchanges, a.t_from, a.t_to)
            if chunk.empty:
                continue
            if a.limit:
                chunk = chunk.iloc[: max(0, a.limit - sent)]
            ts = ticks.trade_timestamps_ms(chunk)
            window = np.where(ts >= 0, ts // ticks.WINDOW_MS, -1)
            if not started:
                started = True
                if not a.no_sos:
                    valid = ts[ts >= 0]
                    announce_start(p, topic, int(valid.min() // ticks.WINDOW_MS * ticks.WINDOW_MS) if len(valid) else 0,
                                   a.start_pause)
                    t_start = last_report = time.time()   # the pause doesn't count towards throughput

            values = (chunk["ID"] + "," + chunk["SecType"] + "," + chunk["Date"] + "," + chunk["Time"] + ","
                      + chunk["Trading time"] + "," + chunk["Last"] + "," + chunk["seq"].astype(str)).str.encode("utf-8")
            keys = chunk["ID"].str.encode("utf-8")

            for i, (k, v) in enumerate(zip(keys.to_numpy(), values.to_numpy())):
                w = window[i]
                if w > max_window:
                    # first event of a new window: every earlier window is now complete
                    if max_window >= 0 and a.boundaries:
                        boundaries.append(((max_window + 1) * ticks.WINDOW_MS, int(time.time() * 1000)))
                    max_window = w
                send(p, topic, k, v)
                sent += 1

                if sent % 2000 == 0:
                    p.poll(0)
                    if a.rate:
                        ahead = sent / a.rate - (time.time() - t_start)
                        if ahead > 0:
                            time.sleep(ahead)
                    if a.speed and ts[i] >= 0:
                        if first_ts is None:
                            first_ts = ts[i]
                        ahead = (ts[i] - first_ts) / 1000 / a.speed - (time.time() - t_start)
                        if ahead > 0:
                            time.sleep(ahead)
                    if not a.quiet and time.time() - last_report > 10:
                        el = time.time() - t_start
                        print(f"  {sent:,} events, {sent / el:,.0f}/s", flush=True)
                        last_report = time.time()
            if a.limit and sent >= a.limit:
                break
        if a.limit and sent >= a.limit:
            break

    if not a.no_eos:
        # One marker per partition, far in the future, so the watermark passes every open window
        eos = f"#EOS,{(max_window + 10) * ticks.WINDOW_MS}".encode()
        parts = p.list_topics(topic, timeout=10).topics[topic].partitions
        for part in parts:
            p.produce(topic, value=eos, partition=part)
        if a.boundaries and max_window >= 0:
            boundaries.append(((max_window + 1) * ticks.WINDOW_MS, int(time.time() * 1000)))
    p.flush(120)
    elapsed = time.time() - t_start

    stats = {"events": sent, "seconds": round(elapsed, 3), "events_per_second": round(sent / max(elapsed, 1e-9)),
             "files": [str(x) for x in paths], "events_kind": a.events, "exchanges": a.exchanges,
             "from": a.t_from, "to": a.t_to, "rate": a.rate, "speed": a.speed, "topic": topic,
             "start_wall_ms": int(t_start * 1000)}
    print(f"sent {sent:,} events in {elapsed:,.1f}s ({stats['events_per_second']:,} events/s)")
    if a.stats:
        Path(a.stats).parent.mkdir(parents=True, exist_ok=True)
        Path(a.stats).write_text(json.dumps(stats, indent=2))
    if a.boundaries:
        Path(a.boundaries).parent.mkdir(parents=True, exist_ok=True)
        with open(a.boundaries, "w") as f:
            f.write("window_end_ms,wall_ms\n")
            f.writelines(f"{w},{t}\n" for w, t in boundaries)


if __name__ == "__main__":
    main()
