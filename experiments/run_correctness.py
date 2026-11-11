#!/usr/bin/env python3
"""
Correctness check: run data through the real Kafka + Flink pipeline and compare
every window with the pandas answer key.

  python experiments/run_correctness.py
      Built-in test slice (1,917 trades, 330 windows). Needs no dataset. About a minute.

  python experiments/run_correctness.py --days 08-11-21
      All of Monday against answer_key/answer-key-week.parquet.
      Needs data/debs2022-gc-trading-day-08-11-21.csv.

  python experiments/run_correctness.py --days 08-11-21,09-11-21,10-11-21,11-11-21,12-11-21
      The whole trading week. The averages carry over from day to day, so always
      start on Monday and don't skip a day.

Exit code 0 means every window, average and signal matched.
"""

import argparse
import sys
import time

from common import (ROOT, compose, container_path, fresh_infrastructure, start_flink, submit_job,
                    tools, wait_for_job)


def main():
    p = argparse.ArgumentParser(description="Compare the pipeline with the answer key.")
    p.add_argument("--days", help="comma-separated days as in the file names, e.g. 08-11-21,09-11-21")
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--max-out-of-order-ms", type=int, default=5000)
    p.add_argument("--timeout", type=int, default=3600)
    p.add_argument("--name", default=time.strftime("correctness-%Y%m%d-%H%M%S"))
    p.add_argument("--keep", action="store_true", help="leave the containers running afterwards")
    args = p.parse_args()

    if args.days:
        days = [d.strip() for d in args.days.split(",")]
        files = [container_path(f"data/debs2022-gc-trading-day-{d}.csv") for d in days]
        expected = ["--expected", "answer_key/answer-key-week.parquet"]
        for d in days:                                  # 08-11-21 -> 2021-11-08
            dd, mm, yy = d.split("-")
            expected += ["--day", f"20{yy}-{mm}-{dd}"]
    else:
        files = ["analysis/test-slice.csv"]
        expected = ["--expected", "analysis/test-slice-expected.jsonl"]

    out = f"experiments/results/{args.name}"
    (ROOT / out).mkdir(parents=True, exist_ok=True)

    print("\n== 1/3  Clean Kafka, load the trades ==")
    fresh_infrastructure()
    tools("python", "producer/producer.py", *files, "--trades-only", "--no-flush-marker",
          "--summary", f"{out}/load.json")

    print("\n== 2/3  Run the Flink job over the topic ==")
    start_flink(args.workers)
    job_id = submit_job(args.workers, "--bounded", "true", "--run-id", args.name, "--subscribe", "ALL",
                        "--max-out-of-order-ms", str(args.max_out_of_order_ms))
    info = wait_for_job(job_id, args.timeout)
    print(f"   {info['state']} in {info['duration'] / 1000:.1f} s")
    if info["state"] != "FINISHED":
        sys.exit("The Flink job did not finish. See: docker compose logs taskmanager")

    print("\n== 3/3  Collect the output and compare ==")
    tools("python", "tools/collect.py", "--out", f"{out}/raw")
    result = tools("python", "tools/compare.py", "--results", f"{out}/raw/ema-results.jsonl",
                   *expected, "--report", f"{out}/comparison.json", check=False)
    late = sum(1 for _ in open(ROOT / out / "raw" / "late-trades.jsonl", encoding="utf-8"))
    print(f"\nTrades Flink dropped as late: {late}"
          + ("" if late == 0 else "  (see raw/late-trades.jsonl; a larger --max-out-of-order-ms keeps them)"))
    if not args.keep:
        compose("down", "-v")
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
