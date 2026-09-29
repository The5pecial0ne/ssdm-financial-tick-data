import pandas as pd

DAYS = ["08-11-21", "09-11-21", "10-11-21", "11-11-21", "12-11-21"]   # weekend has no trades

# 1. One file for the whole week, plus a small signals-only CSV to share with the Flink side
key = pd.concat([pd.read_parquet(f"../data/answer-key-v2-{d}.parquet") for d in DAYS], ignore_index=True)
key.to_parquet("../data/answer-key-week.parquet", index=False)

signals = key[key["signal"] != ""]
signals[["ID", "window_start", "close", "ema38", "ema100", "signal", "first_window"]].to_csv(
    "../data/signals-week.csv", index=False, float_format="%.10f")   # 10 decimals so near-ties survive
print(f"week: {len(key):,} symbol-windows, {len(signals):,} signals "
      f"({(~signals['first_window']).sum():,} real + {signals['first_window'].sum():,} first-window)")

# 2. The crossovers before 07:00: what trades are behind them?
real = signals[~signals["first_window"]]
night = real[real["window_start"].dt.hour < 7]
print(night.to_string())

for _, r in night.iterrows():
    day = r["window_start"].strftime("%d-%m-%y")
    p = pd.read_parquet(f"../data/prices-{day}.parquet")
    p = p[(p["ID"] == r["ID"]) & (p["Trading time"] < "07:00")]   # that symbol's early trades that day
    print(f"\n{r['ID']} on {day}: {len(p)} trades before 07:00")
    print(p[["SecType", "Time", "Trading time", "Last", "Trading date"]].head(10).to_string())