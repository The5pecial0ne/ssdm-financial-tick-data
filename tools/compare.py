#!/usr/bin/env python3
"""
Checks the pipeline's output against the pandas answer key.

Both sides are matched on (symbol, window_start). For every pair it checks
that close, EMA38 and EMA100 agree (relative difference at most --tolerance)
and that the signal is the same. It also lists windows that only one side has.

    python tools/compare.py --results results.jsonl --expected analysis/test-slice-expected.jsonl
    python tools/compare.py --results monday.jsonl --expected answer_key/answer-key-week.parquet --day 2021-11-08

The exit code is 0 only when everything matches, so scripts can rely on it.
"""

import argparse
import json
import sys

import pandas as pd

KEY = ["symbol", "window_start"]


def load(path):
    if path.endswith(".parquet"):
        df = pd.read_parquet(path)
    else:
        df = pd.read_json(path, lines=True, dtype={"signal": str}, convert_dates=False)
    df = df.rename(columns={"ID": "symbol"})
    df["window_start"] = pd.to_datetime(df["window_start"]).astype("datetime64[ns]")
    df["signal"] = df["signal"].fillna("").astype(str)
    return df


def main():
    p = argparse.ArgumentParser(description="Compare pipeline output with the answer key.")
    p.add_argument("--results", required=True, help="JSON Lines from tools/collect.py or LocalFileJob")
    p.add_argument("--expected", required=True, help="answer key, .parquet or .jsonl")
    p.add_argument("--day", action="append", help="only compare this day (YYYY-MM-DD); repeatable")
    p.add_argument("--run-id", help="only use result lines with this run_id")
    p.add_argument("--symbols", help="only compare these symbols (comma-separated), e.g. the subscribed ones")
    p.add_argument("--tolerance", type=float, default=1e-9, help="allowed relative EMA difference")
    p.add_argument("--report", help="write the summary to this JSON file")
    p.add_argument("--show", type=int, default=10, help="how many problem rows to print")
    args = p.parse_args()

    got, want = load(args.results), load(args.expected)

    if "ts" in got.columns:                       # LocalFileJob mixes late trades into the same file
        got = got[got["ts"].isna()]
    if args.run_id and "run_id" in got.columns:
        got = got[got["run_id"] == args.run_id]
    if args.day:
        days = pd.to_datetime(args.day).date
        got = got[got["window_start"].dt.date.isin(days)]
        want = want[want["window_start"].dt.date.isin(days)]
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
        got, want = got[got["symbol"].isin(symbols)], want[want["symbol"].isin(symbols)]

    duplicates = int(got.duplicated(KEY).sum())
    got = got.drop_duplicates(KEY, keep="last")

    m = want.merge(got, on=KEY, how="outer", suffixes=("_exp", "_got"), indicator=True)
    both = m[m["_merge"] == "both"]
    missing = m[m["_merge"] == "left_only"]
    extra = m[m["_merge"] == "right_only"]

    def rel_diff(col):
        a, b = both[f"{col}_exp"].astype(float), both[f"{col}_got"].astype(float)
        return ((a - b).abs() / a.abs().clip(lower=1e-300))

    worst = pd.concat([rel_diff("close"), rel_diff("ema38"), rel_diff("ema100")], axis=1).max(axis=1)
    bad_numbers = both[worst > args.tolerance]
    bad_signals = both[both["signal_exp"] != both["signal_got"]]
    bad_first = both[both["first_window_exp"].astype(bool) != both["first_window_got"].astype(bool)]

    real = lambda df, side: df[(df[f"signal{side}"] != "") & ~df[f"first_window{side}"].astype(bool)]
    summary = {
        "expected_windows": int(len(want)),
        "result_windows": int(len(got)),
        "matched_windows": int(len(both)),
        "missing_from_results": int(len(missing)),
        "not_in_answer_key": int(len(extra)),
        "duplicate_result_windows": duplicates,
        "ema_or_close_mismatches": int(len(bad_numbers)),
        "largest_relative_difference": float(worst.max()) if len(both) else None,
        "signal_mismatches": int(len(bad_signals)),
        "first_window_mismatches": int(len(bad_first)),
        "expected_crossovers": int(len(real(want.rename(columns={"signal": "signal_x", "first_window": "first_window_x"}), "_x"))),
        "result_crossovers": int(len(real(got.rename(columns={"signal": "signal_x", "first_window": "first_window_x"}), "_x"))),
        "symbols": int(want["symbol"].nunique()),
        "tolerance": args.tolerance,
    }
    summary["pass"] = (len(want) > 0 and len(missing) == 0 and len(extra) == 0 and duplicates == 0
                       and len(bad_numbers) == 0 and len(bad_signals) == 0 and len(bad_first) == 0)

    for name, value in summary.items():
        print(f"{name:>28}: {value}")

    def show(title, df, cols):
        if len(df):
            print(f"\n{title} (first {min(args.show, len(df))} of {len(df)}):")
            print(df.sort_values(KEY)[KEY + cols].head(args.show).to_string(index=False))

    show("Windows missing from the results", missing, ["close_exp", "signal_exp"])
    show("Windows the answer key doesn't have", extra, ["close_got", "signal_got"])
    show("EMA or close differs", bad_numbers, ["ema38_exp", "ema38_got", "ema100_exp", "ema100_got"])
    show("Signal differs", bad_signals, ["signal_exp", "signal_got"])

    if args.report:
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
    print("\nRESULT:", "PASS, the pipeline matches the answer key" if summary["pass"] else "FAIL")
    sys.exit(0 if summary["pass"] else 1)


if __name__ == "__main__":
    main()
