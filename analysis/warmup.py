import pandas as pd
import matplotlib.pyplot as plt

A38, A100 = 2 / (1 + 38), 2 / (1 + 100)
key = pd.read_parquet("../data/answer-key-08-11-21.parquet")   # sorted by symbol, then window

# A. How far has EMA100 climbed toward the price by each symbol's last window?
last = key.groupby("ID").tail(1)
print("EMA100 / close at each symbol's last window:\n", (last["ema100"] / last["close"]).describe())

# B. Alternative: start both EMAs at the symbol's first close instead of 0
e38s, e100s, current = [], [], None
for symbol, close in zip(key["ID"], key["close"]):
    if symbol != current:                  # first window: seed with the price itself
        current, e38, e100 = symbol, close, close
    else:
        e38 = close * A38 + e38 * (1 - A38)
        e100 = close * A100 + e100 * (1 - A100)
    e38s.append(e38)
    e100s.append(e100)
key["s38"], key["s100"] = e38s, e100s

g = key.groupby("ID")
p38, p100 = g["s38"].shift(), g["s100"].shift()   # NaN on first windows, so no signal there
buys = ((key["s38"] > key["s100"]) & (p38 <= p100)).sum()
sells = ((key["s38"] < key["s100"]) & (p38 >= p100)).sum()
print(f"seeded variant: {buys:,} buys, {sells:,} sells")

# C. Picture of the warm-up for the busiest symbol
sym = key["ID"].value_counts().idxmax()
one = key[key["ID"] == sym]
fig, ax = plt.subplots(figsize=(10, 4))
ax.plot(one["window_start"], one["close"], label="close", color="black", linewidth=1)
ax.plot(one["window_start"], one["ema38"], label="EMA38 (starts at 0)")
ax.plot(one["window_start"], one["ema100"], label="EMA100 (starts at 0)")
ax.set_title(f"{sym} on Mon 8 Nov 2021: EMAs warming up from 0")
ax.set_ylabel("price")
ax.legend()
fig.tight_layout()
fig.savefig("warmup-A169S8.png", dpi=150)
print("saved warmup-A169S8.png")