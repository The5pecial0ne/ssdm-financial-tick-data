"""
Generates a synthetic day file in the exact DEBS 2022 CSV layout.

This is a test tool, not a stand-in for the real data in the report. It exists so the pipeline
can be stress-tested anywhere (CI, a machine without the dataset) on the awkward cases the real
data has: thousands of symbols with a long-tail popularity, quotes mixed in with trades,
placeholder trading times, out-of-order trades and identical timestamps.

Symbols and security types are taken from the shipped answer key, ranked by how many windows
they trade in, and given Zipf-shaped activity so a few symbols carry most of the traffic.

    python -m tools.synth -n 5000000 -o data/synthetic-08-11-21.csv
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from tools import ticks

REPO = ticks.REPO
HEADER_SOURCE = REPO / "analysis" / "test-slice.csv"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-n", "--events", type=int, default=2_000_000)
    ap.add_argument("-o", "--output", required=True)
    ap.add_argument("--date", default="08-11-2021", help="DD-MM-YYYY")
    ap.add_argument("--start", default="08:00", help="first trading time, HH:MM")
    ap.add_argument("--end", default="17:30")
    ap.add_argument("--quotes", type=float, default=0.3, help="share of events that are quotes, not trades")
    ap.add_argument("--placeholders", type=float, default=0.002, help="share of trades stamped 01:00:00.000")
    ap.add_argument("--out-of-order", type=float, default=0.02, help="share of trades that arrive a bit late")
    ap.add_argument("--max-delay-ms", type=int, default=4000, help="how late those may be (keep under --ooo-ms)")
    ap.add_argument("--zipf", type=float, default=1.1, help="skew of symbol popularity")
    ap.add_argument("--symbols", type=int, default=0, help="use only the N busiest symbols (0 = all)")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    rng = np.random.default_rng(a.seed)

    # ---- symbols with long-tail popularity
    key = pd.read_parquet(REPO / "answer_key" / "answer-key-week.parquet", columns=["ID", "SecType"])
    ranked = key.groupby(["ID", "SecType"]).size().sort_values(ascending=False).reset_index()
    if a.symbols:
        ranked = ranked.head(a.symbols)
    weights = 1.0 / np.arange(1, len(ranked) + 1) ** a.zipf
    weights /= weights.sum()
    sym_idx = rng.choice(len(ranked), size=a.events, p=weights)
    ids = ranked["ID"].to_numpy()[sym_idx]
    sectypes = ranked["SecType"].to_numpy()[sym_idx]

    # ---- arrival times (file order), then trading times that are sometimes a little older
    t0 = pd.to_timedelta(a.start + ":00").total_seconds() * 1000
    t1 = pd.to_timedelta(a.end + ":00").total_seconds() * 1000
    arrival = np.sort(rng.uniform(t0, t1, a.events)).astype(np.int64)
    delay = np.zeros(a.events, dtype=np.int64)
    late = rng.random(a.events) < a.out_of_order
    delay[late] = np.minimum(rng.exponential(a.max_delay_ms / 4, late.sum()), a.max_delay_ms).astype(np.int64)
    trading = arrival - delay
    whole = rng.random(a.events) < 0.1                # round some to the second, which creates ties
    trading[whole] = (trading[whole] // 1000) * 1000

    is_trade = rng.random(a.events) >= a.quotes
    placeholder = is_trade & (rng.random(a.events) < a.placeholders)

    # ---- prices: an independent random walk per symbol
    base = np.exp(rng.normal(3.5, 1.5, len(ranked)))
    steps = rng.normal(0, 0.0008, a.events)
    order = np.argsort(sym_idx, kind="stable")
    walk = np.empty(a.events)
    s_sorted, steps_sorted = sym_idx[order], steps[order]
    cs = np.cumsum(steps_sorted)
    starts = np.r_[0, np.flatnonzero(np.diff(s_sorted)) + 1]
    group_offset = np.repeat(cs[starts] - steps_sorted[starts], np.diff(np.r_[starts, a.events]))
    walk[order] = cs - group_offset
    price = np.round(base[sym_idx] * np.exp(walk), 4)

    def fmt(ms):
        ms = pd.Series(ms).astype(np.int64)
        h, rest = ms // 3_600_000, ms % 3_600_000
        m, rest = rest // 60_000, rest % 60_000
        sec, milli = rest // 1000, rest % 1000
        return (h.astype(str).str.zfill(2) + ":" + m.astype(str).str.zfill(2) + ":"
                + sec.astype(str).str.zfill(2) + "." + milli.astype(str).str.zfill(3))

    header_line = next(l for l in open(HEADER_SOURCE, encoding="utf-8") if l.startswith("ID,"))
    ncols = len(header_line.rstrip("\n").split(","))
    out = pd.DataFrame({"c0": ids, "c1": sectypes, "c2": a.date,
                        "c3": fmt(pd.Series((arrival // 1000) * 1000)).to_numpy()})
    last = np.where(is_trade, pd.Series(price).map("{:.4f}".format).to_numpy(), "")
    ttime = np.where(placeholder, ticks.PLACEHOLDER, fmt(pd.Series(trading)).to_numpy())
    ttime = np.where(is_trade, ttime, "")
    ask = np.where(is_trade, "", pd.Series(price * 1.001).map("{:.4f}".format).to_numpy())
    bid = np.where(is_trade, "", pd.Series(price * 0.999).map("{:.4f}".format).to_numpy())

    cols = {i: "" for i in range(4, ncols)}
    df = pd.DataFrame({**{k: out[k] for k in out}, **{f"c{i}": "" for i in cols}})
    df["c4"], df["c6"], df["c21"], df["c23"] = ask, bid, last, ttime
    df = df[[f"c{i}" for i in range(ncols)]]

    Path(a.output).parent.mkdir(parents=True, exist_ok=True)
    with open(a.output, "w", encoding="utf-8") as f:
        f.write("# Synthetic test data in the DEBS 2022 Grand Challenge layout (generated by tools/synth.py)\n")
        f.write(header_line)
        df.to_csv(f, header=False, index=False)
    print(f"wrote {a.events:,} events ({is_trade.sum():,} trades, {placeholder.sum():,} placeholders, "
          f"{late.sum():,} out of order, {len(ranked):,} symbols) to {a.output}")


if __name__ == "__main__":
    main()
