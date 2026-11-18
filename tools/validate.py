"""
Checks the Flink output against the pandas reference, window by window.

The Flink side is a JSON-lines file (from file mode, or dumped from Kafka with
tools/dump_topic.py). The reference side is one of:
    --answer-key   the shipped parquet answer key (answer_key/answer-key-week.parquet)
    --reference    a parquet/jsonl made by tools/reference.py for the same input
    --csv          raw CSV file(s): the reference is recomputed on the spot

Usage, from the repo root:
    python -m tools.validate --flink results/test-slice-flink.jsonl --csv analysis/test-slice.csv
    python -m tools.validate --flink results/week-flink.jsonl --answer-key answer_key/answer-key-week.parquet

It prints a summary, writes it as JSON next to the Flink file (or to --report), and exits with
code 1 if anything disagrees, so it can run as an automated check.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from tools import reference

REL_TOL = 1e-9    # both sides do the same floating-point operations, so they should agree to the last bits


def load_flink(path: str) -> pd.DataFrame:
    df = pd.read_json(path, lines=True, dtype={"symbol": str, "signal": str})
    if df.empty:
        sys.exit(f"{path} has no results")
    df["signal"] = df["signal"].fillna("")
    return df.rename(columns={"windowStart": "window_start_ms", "firstWindow": "first_window"})


def load_reference(a) -> pd.DataFrame:
    if a.csv:
        ref = reference.compute([Path(p) for p in a.csv])
    elif a.answer_key:
        ref = pd.concat([pd.read_parquet(p) for p in a.answer_key], ignore_index=True)
        ref = ref.rename(columns={"ID": "symbol", "SecType": "secType"})
        ref["window_start_ms"] = ref["window_start"].astype("datetime64[ms]").astype(np.int64)
    else:
        p = a.reference
        ref = pd.read_json(p, lines=True) if p.endswith(".jsonl") else pd.read_parquet(p)
    ref["signal"] = ref["signal"].fillna("")
    return ref[["symbol", "window_start_ms", "close", "ema38", "ema100", "first_window", "signal"]]


def compare(flink: pd.DataFrame, ref: pd.DataFrame) -> dict:
    m = ref.merge(flink, on=["symbol", "window_start_ms"], how="outer",
                  suffixes=("_ref", "_flink"), indicator=True)
    both = m[m["_merge"] == "both"]

    def rel_err(col):
        r, f = both[f"{col}_ref"].to_numpy(float), both[f"{col}_flink"].to_numpy(float)
        return np.abs(f - r) / np.maximum(np.abs(r), 1e-12)

    errs = {c: rel_err(c) for c in ("close", "ema38", "ema100")}
    value_bad = np.zeros(len(both), dtype=bool)
    for e in errs.values():
        value_bad |= e > REL_TOL

    sig_ref, sig_fl = both["signal_ref"].to_numpy(), both["signal_flink"].to_numpy()
    real = ~both["first_window_ref"].astype(bool).to_numpy()

    def signal_stats(mask):
        r, f = sig_ref[mask], sig_fl[mask]
        return {
            "expected": int((r != "").sum()),
            "matched": int(((r != "") & (r == f)).sum()),
            "missed": int(((r != "") & (f == "")).sum()),
            "spurious": int(((r == "") & (f != "")).sum()),
            "wrong_direction": int(((r != "") & (f != "") & (r != f)).sum()),
        }

    only_ref = m[m["_merge"] == "left_only"]
    only_flink = m[m["_merge"] == "right_only"]
    report = {
        "windows_reference": int(len(ref)),
        "windows_flink": int(len(flink)),
        "windows_matched": int(len(both)),
        "windows_missing_in_flink": int(len(only_ref)),
        "windows_extra_in_flink": int(len(only_flink)),
        "symbols_reference": int(ref["symbol"].nunique()),
        "symbols_flink": int(flink["symbol"].nunique()),
        "windows_with_value_mismatch": int(value_bad.sum()),
        "max_rel_error": {c: float(e.max()) if len(e) else 0.0 for c, e in errs.items()},
        "first_window_flag_mismatch": int((both["first_window_ref"].astype(bool)
                                          != both["first_window_flink"].astype(bool)).sum()),
        "signals_all": signal_stats(np.ones(len(both), dtype=bool)),
        "signals_excluding_first_windows": signal_stats(real),
        "examples_missing": only_ref[["symbol", "window_start_ms"]].head(5).to_dict("records"),
        "examples_extra": only_flink[["symbol", "window_start_ms"]].head(5).to_dict("records"),
        "examples_value_mismatch": both.loc[value_bad, ["symbol", "window_start_ms", "close_ref", "close_flink",
                                                        "ema38_ref", "ema38_flink"]].head(5).to_dict("records"),
    }
    s = report["signals_all"]
    report["passed"] = (report["windows_missing_in_flink"] == 0 and report["windows_extra_in_flink"] == 0
                        and report["windows_with_value_mismatch"] == 0
                        and s["missed"] == 0 and s["spurious"] == 0 and s["wrong_direction"] == 0)
    return report


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--flink", required=True, help="Flink output, JSON lines")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--csv", action="append", help="raw CSV input(s); recompute the reference")
    src.add_argument("--answer-key", action="append", help="answer-key parquet file(s)")
    src.add_argument("--reference", help="output of tools/reference.py")
    ap.add_argument("--symbols-from-flink", action="store_true",
                    help="only compare symbols Flink published (use when the run had a subscription list)")
    ap.add_argument("--report", help="where to write the JSON summary")
    a = ap.parse_args()

    flink = load_flink(a.flink)
    ref = load_reference(a)
    if a.symbols_from_flink:
        ref = ref[ref["symbol"].isin(set(flink["symbol"]))]
    rep = compare(flink, ref)

    out = a.report or str(Path(a.flink).with_suffix("")) + ".validation.json"
    Path(out).write_text(json.dumps(rep, indent=2, default=str))

    print(f"windows: reference {rep['windows_reference']:,} | Flink {rep['windows_flink']:,} | "
          f"matched {rep['windows_matched']:,} | missing {rep['windows_missing_in_flink']:,} | "
          f"extra {rep['windows_extra_in_flink']:,}")
    print(f"values: {rep['windows_with_value_mismatch']:,} windows differ beyond {REL_TOL:g} relative; "
          f"max relative error {rep['max_rel_error']}")
    for k in ("signals_all", "signals_excluding_first_windows"):
        print(f"{k}: {rep[k]}")
    print("PASSED" if rep["passed"] else "FAILED", "->", out)
    sys.exit(0 if rep["passed"] else 1)


if __name__ == "__main__":
    main()
