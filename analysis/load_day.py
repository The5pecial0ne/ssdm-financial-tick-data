import sys
import time
import pandas as pd
import os

day = sys.argv[1]                               # e.g. 09-11-21
PATH = f"../data/debs2022-gc-trading-day-{day}.csv"
COLS = ["ID", "SecType", "Date", "Time", "Last", "Trading time", "Trading date"]

reader = pd.read_csv(
    PATH, comment="#", index_col=False, usecols=COLS,
    dtype={"Last": "float64", "Date": "string", "Time": "string",
           "Trading time": "string", "Trading date": "string"},
    chunksize=2_000_000,
)

all_counts, chunks, total, t0 = pd.Series(dtype="int64"), [], 0, time.time()
for chunk in reader:
    chunk["seq"] = range(total, total + len(chunk))       # file position, for tie-breaks
    total += len(chunk)
    all_counts = all_counts.add(chunk["ID"].value_counts(), fill_value=0)
    chunks.append(chunk[chunk["Last"] > 0])                # real trades only

prices = pd.concat(chunks, ignore_index=True)
all_counts = all_counts.astype("int64").sort_values(ascending=False)
prices.to_parquet(f"../data/prices-{day}.parquet", index=False)
all_counts.rename("events").to_frame().to_parquet(f"../data/counts-{day}.parquet")

# ---- Health check: the same things we checked on Monday ----
print(f"{day}: {total:,} events, {len(prices):,} trades ({len(prices) / total:.2%}), "
      f"{len(all_counts):,} symbols seen, {prices['ID'].nunique():,} traded, {time.time() - t0:.0f}s")
print(prices["Date"].value_counts(dropna=False))
print(prices["Trading date"].value_counts(dropna=False).head())

tt = pd.to_timedelta(prices["Trading time"])
lag = (pd.to_timedelta(prices["Time"]) - tt).dt.total_seconds()
print("clocks more than 1 s apart:", (lag.abs() > 1).sum())

behind = (tt.groupby(prices["ID"]).cummax() - tt).dt.total_seconds()
print("out-of-order trades within a symbol:", (behind > 0).sum())
print(prices["Trading time"].str[:2].value_counts().sort_index())

# Row-count check: every line except 11 licence lines, 1 header and 1 description must be an event
with open(PATH, "rb") as f:
    lines = sum(block.count(b"\n") for block in iter(lambda: f.read(1 << 24), b""))
ok = total == lines - 13
print(f"row check: {lines:,} lines - 13 = {lines - 13:,} expected, {total:,} read -> "
      f"{'OK' if ok else 'MISMATCH'}")
if "--delete" in sys.argv:
    if ok:
        os.remove(PATH)
        print("deleted", PATH)
    else:
        print("row check failed, CSV kept for inspection")