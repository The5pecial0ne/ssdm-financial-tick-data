import sys
import pandas as pd

A38, A100 = 2 / (1 + 38), 2 / (1 + 100)
DAYS = sys.argv[1:]                     # all days, in date order
PLACEHOLDER = "01:00:00.000"            # missing trade time (midnight UTC shown in CET)

state = {}      # symbol -> (EMA38, EMA100) after its latest window; Flink keeps exactly this
results = {}    # day -> that day's answer key

for day in DAYS:
    t = pd.read_parquet(f"../data/prices-{day}.parquet",
                        columns=["ID", "SecType", "Date", "Last", "Trading time", "seq"])

    # New in v3: drop trades whose trading time is the placeholder
    bad = t["Trading time"] == PLACEHOLDER
    if bad.any():
        print(f"{day}: dropping {bad.sum():,} trades stamped {PLACEHOLDER} "
              f"({t.loc[bad, 'ID'].nunique():,} symbols)")
    t = t[~bad]

    if t.empty:
        print(f"{day}: no trades (markets closed), EMA state carried over unchanged")
        continue

    t["ts"] = pd.to_datetime(t["Date"], format="%d-%m-%Y") + pd.to_timedelta(t["Trading time"])
    t["window_start"] = t["ts"].dt.floor("5min")

    t = t.sort_values(["ID", "ts", "seq"])
    closes = (t.drop_duplicates(subset=["ID", "window_start"], keep="last")
                [["ID", "SecType", "window_start", "Last"]]
                .rename(columns={"Last": "close"})
                .reset_index(drop=True))

    prev38s, prev100s, e38s, e100s, firsts = [], [], [], [], []
    for symbol, close in zip(closes["ID"], closes["close"]):
        firsts.append(symbol not in state)
        prev38, prev100 = state.get(symbol, (0.0, 0.0))
        e38 = close * A38 + prev38 * (1 - A38)
        e100 = close * A100 + prev100 * (1 - A100)
        state[symbol] = (e38, e100)
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

    closes.to_parquet(f"../data/answer-key-v3-{day}.parquet", index=False)
    results[day] = closes

    real = closes[~closes["first_window"]]
    print(f"{day}: {len(closes):,} symbol-windows, {closes['first_window'].sum():,} first windows, "
          f"{(real['signal'] == 'buy').sum():,} buys, {(real['signal'] == 'sell').sum():,} sells")

# ---- What did the fix change compared with v2? ----
print("\nv3 vs v2:")
for day, v3 in results.items():
    v2 = pd.read_parquet(f"../data/answer-key-v2-{day}.parquet")
    a, b = v2.copy(), v3.copy()
    for df in (a, b):
        df["window_start"] = df["window_start"].astype("datetime64[ns]")
    m = a.merge(b, on=["ID", "window_start"], how="outer", suffixes=("_v2", "_v3"), indicator=True)
    both = m[m["_merge"] == "both"]
    ema_changed = ((both["ema38_v2"] != both["ema38_v3"]) | (both["ema100_v2"] != both["ema100_v3"])).sum()
    sig_changed = (both["signal_v2"] != both["signal_v3"]).sum()
    print(f"{day}: windows only in v2 {(m['_merge'] == 'left_only').sum():,} | "
          f"only in v3 {(m['_merge'] == 'right_only').sum():,} | "
          f"EMAs changed {ema_changed:,} ({both.loc[both['ema38_v2'] != both['ema38_v3'], 'ID'].nunique():,} symbols) | "
          f"signals changed {sig_changed:,}")