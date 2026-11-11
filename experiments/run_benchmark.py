#!/usr/bin/env python3
"""
Scalability benchmark: the same data, processed with 1, 2, 4, ... Flink workers.

What one benchmark does
  1. Starts a clean Kafka and loads the chosen slice of data into it, once.
     Every row is loaded, also the ~80% that are not trades.
  2. For each worker count, and several repeats of each:
       - starts a fresh Flink with the TaskManager limited to that many CPU cores
       - runs the job in "bounded" mode: read the whole topic, then stop
       - notes how long Flink says the job took, and samples CPU and memory meanwhile
  3. Collects every result the runs wrote, then builds summary.csv, scaling.csv and plots.

Because the data sits in Kafka before each run starts, the job runs as fast
as it can: events / seconds is the system's maximum throughput.

Examples
  python experiments/run_benchmark.py                       # tiny smoke test with the built-in slice
  python experiments/run_benchmark.py --files data/debs2022-gc-trading-day-08-11-21.csv --from-time 09:00 --to-time 10:00
  python experiments/run_benchmark.py --files data/debs2022-gc-trading-day-08-11-21.csv --workers 1,2,4,8 --repeats 3
"""

import argparse
import json
import platform
import time

from common import (ROOT, StatsSampler, compose, container_path, fresh_infrastructure, host_cpus,
                    start_flink, submit_job, tools, wait_for_job)


def main():
    p = argparse.ArgumentParser(description="Run the scalability benchmark.")
    p.add_argument("--files", nargs="+", default=["analysis/test-slice.csv"],
                   help="CSV files relative to the repo root, e.g. data/debs2022-gc-trading-day-08-11-21.csv")
    p.add_argument("--from-time", help="only rows received at or after HH:MM")
    p.add_argument("--to-time", help="only rows received before HH:MM")
    p.add_argument("--workers", default="1,2,4,8", help="worker counts to compare")
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--partitions", type=int, default=8, help="partitions of the input topic")
    p.add_argument("--max-out-of-order-ms", type=int, default=5000)
    p.add_argument("--timeout", type=int, default=3600, help="seconds to wait for one run")
    p.add_argument("--name", default=time.strftime("bench-%Y%m%d-%H%M%S"))
    p.add_argument("--keep", action="store_true", help="leave the containers running afterwards")
    args = p.parse_args()

    ncpu, machine = host_cpus()
    workers = [int(w) for w in args.workers.split(",")]
    skipped = [w for w in workers if w > ncpu or w > args.partitions]
    workers = [w for w in workers if w not in skipped]
    if skipped:
        print(f"Skipping {skipped}: Docker has {ncpu} CPU cores and the topic has {args.partitions} partitions.")
    if args.files == ["analysis/test-slice.csv"]:
        print("No --files given: using the 1,917-event test slice. Fine as a smoke test, far too small to measure anything.")

    files = [container_path(f) for f in args.files]
    out_dir = ROOT / "experiments" / "results" / args.name
    out_dir.mkdir(parents=True, exist_ok=True)
    in_container = f"experiments/results/{args.name}"

    print("\n== 1/3  Clean Kafka, load the data ==")
    fresh_infrastructure(args.partitions)
    load = ["python", "producer/producer.py", *files, "--no-flush-marker", "--summary", f"{in_container}/load.json"]
    if args.from_time:
        load += ["--from-time", args.from_time]
    if args.to_time:
        load += ["--to-time", args.to_time]
    tools(*load)
    load_summary = json.loads((out_dir / "load.json").read_text(encoding="utf-8"))
    if load_summary["events"] == 0:
        raise SystemExit("No events were loaded. Check --files, --from-time and --to-time.")

    print("\n== 2/3  Runs ==")
    runs = []
    for repeat in range(1, args.repeats + 1):          # repeats on the outside: slow drift (heat) hits all counts alike
        for w in workers:
            run_id = f"{args.name}-w{w}-r{repeat}"
            print(f"\n-- {w} worker(s), repeat {repeat} of {args.repeats} --")
            start_flink(w)
            sampler = StatsSampler()
            sampler.start()
            job_id = submit_job(w, "--bounded", "true", "--run-id", run_id, "--subscribe", "ALL",
                                "--max-out-of-order-ms", str(args.max_out_of_order_ms))
            info = wait_for_job(job_id, args.timeout)
            containers = sampler.stop()
            seconds = info["duration"] / 1000
            print(f"   {info['state']} in {seconds:.1f} s  ->  {load_summary['events'] / seconds:,.0f} events/s")
            runs.append({"run_id": run_id, "parallelism": w, "repeat": repeat, "job_id": job_id,
                         "state": info["state"], "duration_ms": info["duration"],
                         "start_time": info["start-time"], "end_time": info["end-time"],
                         "containers": containers})
            meta = {"name": args.name, "machine": {**machine, "host": platform.platform()},
                    "settings": vars(args), "load": load_summary, "runs": runs}
            (out_dir / "runs.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print("\n== 3/3  Collect results and summarise ==")
    tools("python", "tools/collect.py", "--out", f"{in_container}/raw")
    tools("python", "tools/summarize.py", in_container)
    if not args.keep:
        compose("down", "-v")
    print(f"\nDone. Everything is in {out_dir}")


if __name__ == "__main__":
    main()
