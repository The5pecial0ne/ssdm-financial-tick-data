import pandas as pd

A38 = 2 / (1 + 38)     # weight of the newest close in EMA38
A100 = 2 / (1 + 100)   # weight of the newest close in EMA100
WINDOW = pd.Timedelta(minutes=5)
DAY = pd.Timestamp("2021-11-08")

cols = ["ID", "SecType", "Last", "Trading time", "seq"]
trades = pd.read_parquet("../data/prices-08-11-21.parquet", columns=cols)

# 1. Put every trade in its 5-minute window (window 0 starts 00:00, window 1 at 00:05, ...)
trades["tt"] = pd.to_timedelta(trades["Trading time"])
trades["window"] = trades["tt"] // WINDOW

# 2. Close = the last trade of each symbol in each window.
#    Sort by symbol, then time, then file position (the tie-break), keep the last row per window.
trades = trades.sort_values(["ID", "tt", "seq"])
closes = (trades.drop_duplicates(subset=["ID", "window"], keep="last")
                [["ID", "SecType", "window", "Last"]]
                .rename(columns={"Last": "close"})
                .reset_index(drop=True))

# 3. EMAs, one window at a time, exactly like the spec's formula.
#    A plain loop on purpose: easy to read, and it mirrors what Flink does per symbol.
ema38, ema100 = [], []
current, e38, e100 = None, 0.0, 0.0
for symbol, close in zip(closes["ID"], closes["close"]):
    if symbol != current:            # a new symbol starts from EMA = 0
        current, e38, e100 = symbol, 0.0, 0.0
    e38 = close * A38 + e38 * (1 - A38)
    e100 = close * A100 + e100 * (1 - A100)
    ema38.append(e38)
    ema100.append(e100)
closes["ema38"] = ema38
closes["ema100"] = ema100

# 4. Crossovers: compare this window with the symbol's previous traded window.
by_symbol = closes.groupby("ID")
prev38 = by_symbol["ema38"].shift(fill_value=0.0)    # before the first window both EMAs are 0
prev100 = by_symbol["ema100"].shift(fill_value=0.0)
buy = (closes["ema38"] > closes["ema100"]) & (prev38 <= prev100)
sell = (closes["ema38"] < closes["ema100"]) & (prev38 >= prev100)
closes["signal"] = ""
closes.loc[buy, "signal"] = "buy"
closes.loc[sell, "signal"] = "sell"
closes["first_window"] = by_symbol.cumcount() == 0   # flags the automatic first-window buy

# 5. Readable window start, e.g. 2021-11-08 09:05:00
closes["window_start"] = DAY + closes["window"] * WINDOW

closes.to_parquet("../data/answer-key-08-11-21.parquet", index=False)

# ---- Summary ----
print(f"{len(closes):,} symbol-windows for {closes['ID'].nunique():,} symbols")
print(closes["signal"].value_counts())
print("signals excluding first windows:\n", closes.loc[~closes["first_window"], "signal"].value_counts())
print("windows per symbol:\n", by_symbol.size().describe())
busiest = closes["ID"].value_counts().idxmax()
print(closes[closes["ID"] == busiest].head(12).to_string())