#!/usr/bin/env python3
"""
Turns the raw material of a benchmark into a table and four plots.

Reads, from one results folder:
    runs.json               one entry per Flink run, written by experiments/run_benchmark.py
    raw/ema-results.jsonl   every EMA line of every run   (tools/collect.py)
    raw/advisories.jsonl    every buy/sell advisory
    raw/late-trades.jsonl   trades Flink dropped as late

Writes:
    summary.csv             one row per run
    scaling.csv             one row per worker count (median over the repeats)
    throughput.png, speedup.png, latency.png, cpu.png

Latency here is "detection to output": from the moment Flink computed a result
(detected_at) to the moment the Kafka broker stored it (_kafka_ts).
"""

import argparse
import hashlib
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


def read_jsonl(path):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return pd.DataFrame(columns=["run_id"])
    return pd.read_json(path, lines=True, dtype={"signal": str}, convert_dates=False)


def fingerprint(df):
    """Same windows, same numbers, same signals -> same fingerprint, whatever the arrival order."""
    cols = ["symbol", "window_start", "close", "ema38", "ema100", "signal"]
    rows = df[cols].astype(str).sort_values(["symbol", "window_start"])
    return hashlib.sha256("\n".join(rows.agg(",".join, axis=1)).encode()).hexdigest()[:16]


def main():
    p = argparse.ArgumentParser(description="Summarise a benchmark results folder.")
    p.add_argument("folder")
    args = p.parse_args()
    folder = args.folder

    with open(os.path.join(folder, "runs.json"), encoding="utf-8") as f:
        meta = json.load(f)
    ema = read_jsonl(os.path.join(folder, "raw", "ema-results.jsonl"))
    adv = read_jsonl(os.path.join(folder, "raw", "advisories.jsonl"))
    late = read_jsonl(os.path.join(folder, "raw", "late-trades.jsonl"))

    events = meta["load"]["events"]
    rows = []
    for run in meta["runs"]:
        mine = ema[ema["run_id"] == run["run_id"]]
        latency = (mine["_kafka_ts"] - mine["detected_at"]) if len(mine) else pd.Series(dtype=float)
        seconds = run["duration_ms"] / 1000
        row = {
            "run_id": run["run_id"], "workers": run["parallelism"], "repeat": run["repeat"],
            "state": run["state"], "seconds": round(seconds, 2),
            "events_per_second": round(events / seconds) if seconds else 0,
            "windows": len(mine),
            "advisories": int((adv["run_id"] == run["run_id"]).sum()),
            "late_trades": int((late["run_id"] == run["run_id"]).sum()),
            "latency_p50_ms": latency.quantile(0.50) if len(latency) else None,
            "latency_p95_ms": latency.quantile(0.95) if len(latency) else None,
            "latency_p99_ms": latency.quantile(0.99) if len(latency) else None,
            "latency_max_ms": latency.max() if len(latency) else None,
            "fingerprint": fingerprint(mine) if len(mine) else "",
        }
        for name, stats in run.get("containers", {}).items():
            row[f"{name}_cpu_avg_pct"] = stats["cpu_avg"]
            row[f"{name}_cpu_max_pct"] = stats["cpu_max"]
            row[f"{name}_mem_max_mb"] = stats["mem_max_mb"]
        rows.append(row)

    summary = pd.DataFrame(rows)
    summary.to_csv(os.path.join(folder, "summary.csv"), index=False)

    ok = summary[summary["state"] == "FINISHED"]
    scaling = ok.groupby("workers").agg(
        runs=("seconds", "size"), seconds=("seconds", "median"),
        seconds_min=("seconds", "min"), seconds_max=("seconds", "max"),
        events_per_second=("events_per_second", "median"),
        latency_p50_ms=("latency_p50_ms", "median"), latency_p95_ms=("latency_p95_ms", "median"),
        latency_p99_ms=("latency_p99_ms", "median")).reset_index()
    if len(scaling):
        base = scaling.sort_values("workers").iloc[0]
        scaling["speedup"] = (base["seconds"] / scaling["seconds"]).round(2)
        scaling["efficiency_pct"] = (100 * scaling["speedup"] * base["workers"] / scaling["workers"]).round(0)
    cpu_cols = [c for c in ok.columns if c.endswith("_cpu_avg_pct")]
    if cpu_cols:
        scaling = scaling.merge(ok.groupby("workers")[cpu_cols].median().reset_index(), on="workers")
    scaling.to_csv(os.path.join(folder, "scaling.csv"), index=False)

    print(f"\n{events:,} events per run ({meta['load'].get('trades', 0):,} of them trades)\n")
    print(summary.drop(columns=[c for c in summary.columns if "_cpu_" in c or "_mem_" in c]).to_string(index=False))
    print("\nMedian per worker count:\n" + scaling.to_string(index=False))

    prints = set(ok["fingerprint"]) - {""}
    if len(prints) == 1:
        print("\nEvery run produced exactly the same windows, averages and signals.")
    elif prints:
        print(f"\nWARNING: the runs do NOT all agree ({len(prints)} different outputs). "
              "Check late_trades: a trade dropped as late in one run changes that symbol's averages.")

    if len(scaling) == 0:
        return
    w = scaling["workers"]

    def save(name, ylabel, title):
        plt.xlabel("Flink workers (parallelism = CPU cores given to the TaskManager)")
        plt.ylabel(ylabel)
        plt.title(title)
        plt.xticks(w)
        plt.grid(alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(folder, name), dpi=160)
        plt.close()

    plt.figure(figsize=(6, 4))
    plt.bar(w.astype(str), scaling["events_per_second"])
    plt.xlabel("Flink workers (parallelism = CPU cores given to the TaskManager)")
    plt.ylabel("events per second")
    plt.title("Throughput")
    plt.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(folder, "throughput.png"), dpi=160)
    plt.close()

    plt.figure(figsize=(6, 4))
    plt.plot(w, scaling["speedup"], marker="o", label="measured")
    plt.plot(w, w / w.iloc[0], linestyle="--", color="grey", label="ideal (linear)")
    plt.legend()
    save("speedup.png", f"speedup over {int(w.iloc[0])} worker(s)", "Speedup")

    plt.figure(figsize=(6, 4))
    for col, label in [("latency_p50_ms", "median"), ("latency_p95_ms", "95th percentile"),
                       ("latency_p99_ms", "99th percentile")]:
        plt.plot(w, scaling[col], marker="o", label=label)
    plt.legend()
    save("latency.png", "ms from detection to output", "Latency: result computed -> stored in Kafka")

    if cpu_cols:
        plt.figure(figsize=(6, 4))
        for col in cpu_cols:
            plt.plot(w, scaling[col], marker="o", label=col.replace("_cpu_avg_pct", ""))
        plt.legend()
        save("cpu.png", "average CPU use, % of one core", "CPU use per container")


if __name__ == "__main__":
    main()
