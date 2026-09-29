import sys
import pandas as pd

A38, A100 = 2 / (1 + 38), 2 / (1 + 100)
DAYS = sys.argv[1:]                     # e.g. 08-11-21 09-11-21, in date order

state = {}      # symbol -> (EMA38, EMA100) after its latest window; Flink keeps exactly this
results = []

for day in DAYS:
    t = pd.read_parquet(f"../data/prices-{day}.parquet",
                        columns=["ID", "SecType", "Date", "Last", "Trading time", "seq"])

    if t.empty:
        print(f"{day}: no trades (markets closed), EMA state carried over unchanged")
        continue

    # Full timestamp = the day from Date + the moment from Trading time, then its 5-minute window
    t["ts"] = pd.to_datetime(t["Date"], format="%d-%m-%Y") + pd.to_timedelta(t["Trading time"])
    t["window_start"] = t["ts"].dt.floor("5min")

    # Close = last trade per symbol and window (ties: later file position wins)
    t = t.sort_values(["ID", "ts", "seq"])
    closes = (t.drop_duplicates(subset=["ID", "window_start"], keep="last")
                [["ID", "SecType", "window_start", "Last"]]
                .rename(columns={"Last": "close"})
                .reset_index(drop=True))

    prev38s, prev100s, e38s, e100s, firsts = [], [], [], [], []
    for symbol, close in zip(closes["ID"], closes["close"]):
        firsts.append(symbol not in state)
        prev38, prev100 = state.get(symbol, (0.0, 0.0))   # never seen before: EMAs start at 0
        e38 = close * A38 + prev38 * (1 - A38)
        e100 = close * A100 + prev100 * (1 - A100)
        state[symbol] = (e38, e100)                     # carried on, even across midnight
        prev38s.append(prev38)
        prev100s.append(prev100)
        e38s.append(e38)
        e100s.append(e100)

    closes["ema38"], closes["ema100"] = e38s, e100s
    closes["first_window"] = firsts
    prev38, prev100 = pd.Series(prev38s), pd.Series(prev100s)
    buy = (closes["ema38"] > closes["ema100"]) & (prev38 <= prev100)
    sell = (closes["ema38"] < closes["ema100"]) & (prev38 >= prev100)
    closes["signal"] = ""
    closes.loc[buy, "signal"] = "buy"
    closes.loc[sell, "signal"] = "sell"

    closes.to_parquet(f"../data/answer-key-v2-{day}.parquet", index=False)
    results.append(closes)

    real = closes[~closes["first_window"]]
    print(f"{day}: {len(closes):,} symbol-windows, {closes['first_window'].sum():,} first windows, "
          f"{(real['signal'] == 'buy').sum():,} buys, {(real['signal'] == 'sell').sum():,} sells")

# ---- Regression check: Monday in v2 must match v1 exactly (same data, same rules) ----
v1 = pd.read_parquet("../data/answer-key-08-11-21.parquet")
v2 = results[0].copy()
for df in (v1, v2):
    df["window_start"] = df["window_start"].astype("datetime64[ns]")   # same time resolution for the join
m = v1.merge(v2, on=["ID", "window_start"], suffixes=("_v1", "_v2"))
print("\nMonday rows v1 / v2 / matched:", len(v1), len(v2), len(m))
print("largest EMA difference:", max((m["ema38_v1"] - m["ema38_v2"]).abs().max(),
                                     (m["ema100_v1"] - m["ema100_v2"]).abs().max()))
print("signals identical:", (m["signal_v1"] == m["signal_v2"]).all())

# ---- First look at real crossovers after Monday ----
later = pd.concat(results[1:])
sig = later[(later["signal"] != "") & ~later["first_window"]]
print("\nreal crossovers by hour:\n", sig["window_start"].dt.hour.value_counts().sort_index())
print("most active symbols:\n", sig["ID"].value_counts().head(10))
print(sig.head(10).to_string())