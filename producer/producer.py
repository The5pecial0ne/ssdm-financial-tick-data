#!/usr/bin/env python3
"""
Replays DEBS 2022 tick-data CSV files into Kafka.

Each CSV row becomes one small line with only the five columns the pipeline
needs:

    ID,SecType,Date,Trading time,Last

Things worth knowing:

* Every message is keyed by its symbol. Kafka sends equal keys to the same
  partition, so a symbol's events stay in file order.
* By default every row is sent, including the ~80% that are not trades (bids,
  asks, housekeeping). Flink filters those out, which is the realistic load.
  --trades-only filters here instead, which is quicker for correctness runs.
* When the files are done, one "flush" marker goes to every partition. Its
  timestamp is far in the future, so Flink closes the last open windows
  instead of waiting forever for more data. Give all the days you want to
  replay in ONE call: anything sent after the marker would look late.
* In a paced replay (--rate), a "heartbeat" goes to every partition about once
  a second. It carries the newest trade time sent so far. Without it, a
  partition whose symbols are quiet would hold back every window in the job,
  because Flink waits for the slowest partition before it closes a window.

Examples
    python producer/producer.py analysis/test-slice.csv
    python producer/producer.py data/debs2022-gc-trading-day-08-11-21.csv --from-time 09:00 --to-time 10:00
    python producer/producer.py analysis/test-slice.csv --to-file lines.txt     # no Kafka, just write the lines
"""

import argparse
import json
import os
import sys
import time

FLUSH_SYMBOL = "__FLUSH__"
HEARTBEAT_SYMBOL = "__HEARTBEAT__"
PLACEHOLDER_TIME = "01:00:00.000"
NEEDED = ["ID", "SecType", "Date", "Time", "Last", "Trading time"]


def read_events(path, from_time, to_time, trades_only):
    """
    Yields (symbol, line, is_trade) for each event row of one CSV file.

    The files start with licence lines (#...), then the header, then one more
    comment line describing the columns. Rows are sliced by the receive Time
    column, because rows that are not trades have no Trading time.
    """
    cols = None
    with open(path, "r", encoding="utf-8", newline="") as f:
        for raw in f:
            if raw.startswith("#"):
                continue
            row = raw.rstrip("\r\n").split(",")
            if cols is None:
                names = [name.strip() for name in row]
                try:
                    cols = [names.index(name) for name in NEEDED]
                except ValueError:
                    sys.exit(f"{path}: no column header found. Is this a DEBS 2022 trading CSV?")
                i_id, i_sec, i_date, i_time, i_last, i_tt = cols
                width = max(cols) + 1
                continue
            if len(row) < width:
                continue

            received = row[i_time]
            if from_time and received < from_time:
                continue
            if to_time and received >= to_time:
                continue

            last, trading_time = row[i_last], row[i_tt]
            is_trade = bool(last) and bool(trading_time) and trading_time != PLACEHOLDER_TIME
            if is_trade:
                try:
                    is_trade = float(last) > 0
                except ValueError:
                    is_trade = False
            if trades_only and not is_trade:
                continue

            symbol = row[i_id]
            # Sortable form of the trade's date and time (the date is dd-mm-yyyy), for the heartbeat.
            date = row[i_date]
            when = (date[6:10] + date[3:5] + date[0:2] + trading_time, date, trading_time) if is_trade else None
            yield symbol, f"{symbol},{row[i_sec]},{date},{trading_time},{last}", is_trade, when


class Pacer:
    """Sleeps just enough to hold a target rate of events per second (0 = as fast as possible)."""

    def __init__(self, rate):
        self.rate = rate
        self.every = max(1, int(rate / 20))     # check the clock about 20 times a second
        self.start = time.perf_counter()
        self.sent = 0

    def tick(self):
        self.sent += 1
        if self.rate and self.sent % self.every == 0:
            ahead = self.sent / self.rate - (time.perf_counter() - self.start)
            if ahead > 0:
                time.sleep(ahead)


def main():
    p = argparse.ArgumentParser(description="Replay DEBS 2022 tick data into Kafka.")
    p.add_argument("files", nargs="+", help="day CSV files, in date order")
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--topic", default="trading-events")
    p.add_argument("--from-time", help="only rows received at or after HH:MM[:SS]")
    p.add_argument("--to-time", help="only rows received before HH:MM[:SS]")
    p.add_argument("--trades-only", action="store_true", help="skip rows that are not trades")
    p.add_argument("--rate", type=float, default=0, help="events per second, 0 = as fast as possible")
    p.add_argument("--limit", type=int, default=0, help="stop after this many events")
    p.add_argument("--heartbeat-s", type=float, default=None,
                   help="seconds between heartbeats; default 1 when --rate is set, otherwise off")
    p.add_argument("--no-flush-marker", action="store_true", help="don't send the end-of-data marker")
    p.add_argument("--to-file", help="write the lines to this file instead of Kafka")
    p.add_argument("--summary", help="also write the final counts to this JSON file")
    args = p.parse_args()

    if args.to_file:
        out = open(args.to_file, "w", encoding="utf-8", newline="\n")
        send = lambda symbol, line: out.write(line + "\n")
        finish = out.close
        partitions = [None]
    else:
        from confluent_kafka import Producer

        producer = Producer({
            "bootstrap.servers": args.bootstrap,
            "linger.ms": 20,                    # wait a moment so messages travel in batches
            "batch.num.messages": 10000,
            "compression.type": "lz4",
            "queue.buffering.max.messages": 1000000,
            "enable.idempotence": True,         # no duplicates and no reordering on retries
        })
        meta = producer.list_topics(args.topic, timeout=30).topics[args.topic]
        if meta.error is not None:
            sys.exit(f"Topic {args.topic} is not available: {meta.error}")
        partitions = sorted(meta.partitions)
        failed = []

        def on_delivery(err, msg):
            if err is not None:
                failed.append(str(err))

        def send(symbol, line, partition=None):
            while True:
                try:
                    if partition is None:
                        producer.produce(args.topic, key=symbol, value=line, on_delivery=on_delivery)
                    else:
                        producer.produce(args.topic, key=symbol, value=line,
                                         partition=partition, on_delivery=on_delivery)
                    return
                except BufferError:
                    producer.poll(0.05)         # local queue is full: let it drain a little

        def finish():
            producer.flush()
            if failed:
                sys.exit(f"{len(failed)} messages were not delivered, first error: {failed[0]}")

    heartbeat_s = args.heartbeat_s if args.heartbeat_s is not None else (1.0 if args.rate else 0)
    if args.to_file:
        heartbeat_s = 0                         # a file has no partitions to keep in step
    newest, last_beat = None, time.perf_counter()

    pacer, events, trades, started = Pacer(args.rate), 0, 0, time.time()
    try:
        for path in args.files:
            before = events
            for symbol, line, is_trade, when in read_events(path, args.from_time, args.to_time, args.trades_only):
                send(symbol, line)
                events += 1
                trades += is_trade
                if when and (newest is None or when > newest):
                    newest = when
                pacer.tick()
                if heartbeat_s and newest and time.perf_counter() - last_beat >= heartbeat_s:
                    last_beat = time.perf_counter()
                    beat = f"{HEARTBEAT_SYMBOL},,{newest[1]},{newest[2]},"
                    for partition in partitions:
                        send(HEARTBEAT_SYMBOL, beat, partition)
                if not args.to_file and events % 20000 == 0:
                    producer.poll(0)            # lets delivery reports through
                if events % 2_000_000 == 0:
                    print(f"  {events:,} events sent, {events / (time.time() - started):,.0f}/s", flush=True)
                if args.limit and events >= args.limit:
                    break
            print(f"{path}: {events - before:,} events", flush=True)
            if args.limit and events >= args.limit:
                break

        if not args.no_flush_marker:
            marker = f"{FLUSH_SYMBOL},,,,"
            for partition in partitions:
                if args.to_file:
                    send(FLUSH_SYMBOL, marker)
                else:
                    send(FLUSH_SYMBOL, marker, partition)
    finally:
        finish()

    seconds = time.time() - started
    summary = {"events": events, "trades": trades, "seconds": round(seconds, 2),
               "events_per_second": round(events / seconds) if seconds > 0 else 0,
               "partitions": len(partitions), "files": args.files,
               "from_time": args.from_time, "to_time": args.to_time, "trades_only": args.trades_only}
    if args.summary:
        with open(args.summary, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
    print("SUMMARY " + json.dumps(summary), flush=True)


if __name__ == "__main__":
    main()
