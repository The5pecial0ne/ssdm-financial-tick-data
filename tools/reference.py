"""
The pandas reference implementation (the "answer key" logic from analysis/answer_key_v3.py),
packaged so it can be recomputed for any input: a single test file, a few busy hours,
one exchange, or the whole week.

Usage, from the repo root:
    python -m tools.reference --file analysis/test-slice.csv -o results/ref.parquet
    python -m tools.reference --days weekdays --exchanges FR -o results/ref-FR.parquet
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path

import numpy as np
import pandas as pd

from tools import ticks

A38, A100 = 2 / (1 + 38), 2 / (1 + 100)


def closes_for_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Last trade per (symbol, 5-minute window), using the answer key's rules."""
    t = df[(df["price"] > 0) & (df["Trading time"] != "") & (df["Trading time"] != ticks.PLACEHOLDER)].copy()
    if t.empty:
        return pd.DataFrame(columns=["ID", "SecType", "window_start_ms", "close", "ts", "seq"])
    t["ts"] = ticks.trade_timestamps_ms(t)
    t["window_start_ms"] = (t["ts"] // ticks.WINDOW_MS) * ticks.WINDOW_MS
    t = t.sort_values(["ID", "ts", "seq"], kind="mergesort")
    return (t.drop_duplicates(subset=["ID", "window_start_ms"], keep="last")
             [["ID", "SecType", "window_start_ms", "price", "ts", "seq"]]
             .rename(columns={"price": "close"}))


def ema_and_signals(closes: pd.DataFrame, state: dict) -> pd.DataFrame:
    """Runs EMA38/EMA100 and the crossover rules over closes sorted by symbol and window."""
    closes = closes.sort_values(["ID", "window_start_ms"], kind="mergesort").reset_index(drop=True)
    n = len(closes)
    e38 = np.empty(n)
    e100 = np.empty(n)
    p38 = np.empty(n)
    p100 = np.empty(n)
    first = np.empty(n, dtype=bool)
    for i, (sym, close) in enumerate(zip(closes["ID"].to_numpy(), closes["close"].to_numpy())):
        prev = state.get(sym)
        first[i] = prev is None
        a, b = prev if prev is not None else (0.0, 0.0)
        x = close * A38 + a * (1 - A38)
        y = close * A100 + b * (1 - A100)
        state[sym] = (x, y)
        e38[i], e100[i], p38[i], p100[i] = x, y, a, b
    closes["ema38"], closes["ema100"], closes["first_window"] = e38, e100, first
    buy = (e38 > e100) & (p38 <= p100)
    sell = (e38 < e100) & (p38 >= p100)
    closes["signal"] = np.where(buy, "buy", np.where(sell, "sell", ""))
    return closes


def compute(paths: list[Path], events="trades", exchanges=None, t_from=None, t_to=None) -> pd.DataFrame:
    """Reference results for a sequence of files, oldest first. EMA state carries over between files."""
    state, out = {}, []
    for i, path in enumerate(paths):
        t0 = time.time()
        frames = [ticks.apply_filters(c, events, exchanges, t_from, t_to)
                  for c in ticks.read_chunks(path, seq_offset=i * 10**10)]
        frames = [f for f in frames if len(f)]
        if not frames:
            print(f"{path.name}: no events after filtering")
            continue
        closes = closes_for_frame(pd.concat(frames, ignore_index=True))
        res = ema_and_signals(closes, state)
        out.append(res)
        print(f"{path.name}: {len(res):,} symbol-windows, "
              f"{(res['signal'] != '').sum():,} signals, {time.time() - t0:.0f}s")
    if not out:
        return pd.DataFrame()
    return pd.concat(out, ignore_index=True).rename(columns={"ID": "symbol", "SecType": "secType"})


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--file", action="append", help="CSV file(s) in date order (instead of --days)")
    ap.add_argument("--days", default="weekdays", help="week, weekdays, or e.g. 08-11-21,09-11-21")
    ap.add_argument("--data-dir", default=str(ticks.DATA_DIR))
    ap.add_argument("--exchanges", help="comma-separated, e.g. FR,NL,ETR")
    ap.add_argument("--from", dest="t_from", help="HH:MM, start of a time-of-day range")
    ap.add_argument("--to", dest="t_to", help="HH:MM, end of the range (exclusive)")
    ap.add_argument("-o", "--output", required=True, help=".parquet or .jsonl")
    a = ap.parse_args()

    paths = [Path(f) for f in a.file] if a.file else [ticks.day_file(d, a.data_dir) for d in ticks.resolve_days(a.days)]
    ex = a.exchanges.split(",") if a.exchanges else None
    ref = compute(paths, "trades", ex, a.t_from, a.t_to)
    Path(a.output).parent.mkdir(parents=True, exist_ok=True)
    if a.output.endswith(".jsonl"):
        ref.to_json(a.output, orient="records", lines=True)
    else:
        ref.to_parquet(a.output, index=False)
    print(f"wrote {len(ref):,} rows to {a.output}")


if __name__ == "__main__":
    main()
