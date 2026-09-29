import pandas as pd

DAYS = ["08-11-21", "09-11-21", "10-11-21", "11-11-21", "12-11-21"]

# 1. Package the final answer key (v3)
key = pd.concat([pd.read_parquet(f"../data/answer-key-v3-{d}.parquet") for d in DAYS], ignore_index=True)
key.to_parquet("../data/answer-key-week.parquet", index=False)
signals = key[key["signal"] != ""]
signals[["ID", "window_start", "close", "ema38", "ema100", "signal", "first_window"]].to_csv(
    "../data/signals-week.csv", index=False, float_format="%.10f")
real = signals[~signals["first_window"]]
print(f"week: {len(key):,} symbol-windows, {len(signals):,} signals "
      f"({len(real):,} real + {signals['first_window'].sum():,} first-window)")
print("real crossovers by hour:\n", real["window_start"].dt.hour.value_counts().sort_index())

# 2. v2 vs v3: did crossovers move in time, or appear/disappear?
old = pd.concat([pd.read_parquet(f"../data/answer-key-v2-{d}.parquet") for d in DAYS], ignore_index=True)
old = old[(old["signal"] != "") & ~old["first_window"]]

cols = ["ID", "window_start", "signal"]
m = old[cols].merge(real[cols], how="outer", indicator=True)
only_v2 = m[m["_merge"] == "left_only"].drop(columns="_merge").sort_values("window_start")
only_v3 = m[m["_merge"] == "right_only"].drop(columns="_merge").sort_values("window_start")
print(f"\nidentical crossovers: {(m['_merge'] == 'both').sum():,} | "
      f"only in v2: {len(only_v2):,} | only in v3: {len(only_v3):,}")

# For each v2-only crossover, find the nearest v3-only crossover of the same symbol and direction
near = pd.merge_asof(only_v2, only_v3.rename(columns={"window_start": "ws_v3"}),
                     left_on="window_start", right_on="ws_v3",
                     by=["ID", "signal"], direction="nearest")
near["shift_min"] = (near["ws_v3"] - near["window_start"]).dt.total_seconds() / 60
print("v2-only crossovers with no v3 partner (truly vanished):", near["ws_v3"].isna().sum())
print("time shift to the partner, minutes:\n", near["shift_min"].describe())