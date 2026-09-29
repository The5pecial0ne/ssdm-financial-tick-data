import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

DAYS = ["08-11-21", "09-11-21", "10-11-21", "11-11-21", "12-11-21", "13-11-21", "14-11-21"]

# Events per symbol (every event type), summed over the week
events = (pd.concat([pd.read_parquet(f"../data/counts-{d}.parquet")["events"] for d in DAYS])
            .groupby(level=0).sum().sort_values(ascending=False))

# Trades per symbol (Last > 0 only), summed over the week
trades = pd.concat([pd.read_parquet(f"../data/prices-{d}.parquet", columns=["ID"])["ID"]
                    for d in DAYS]).value_counts()

# ---- Headline numbers ----
for name, s in [("all events", events), ("trades", trades)]:
    share = s.cumsum() / s.sum()                          # running share of the total, busiest first
    top1 = s.iloc[: max(1, len(s) // 100)].sum() / s.sum()
    top10 = s.iloc[: max(1, len(s) // 10)].sum() / s.sum()
    half = (share < 0.5).sum() + 1                        # symbols needed to reach 50%
    print(f"{name}: {s.sum():,} total over {len(s):,} symbols | top 1% -> {top1:.1%} | "
          f"top 10% -> {top10:.1%} | {half:,} symbols make half | "
          f"busiest: {s.index[0]} ({s.iloc[0]:,}) | median per symbol: {s.median():,.0f}")

# ---- Plot: two panels side by side, never one chart with two y-axes ----
fig, (left, right) = plt.subplots(1, 2, figsize=(11, 4.2))
for s, label, color in [(events, "all events", "#6b7280"), (trades, "trades only", "#2563eb")]:
    rank = np.arange(1, len(s) + 1)
    left.loglog(rank, s.values, color=color, linewidth=2, label=label)
    right.plot(rank / len(s) * 100, s.cumsum().values / s.sum() * 100,
               color=color, linewidth=2, label=label)

left.set_title("Events per symbol, busiest first")
left.set_xlabel("symbol rank (1 = busiest, log scale)")
left.set_ylabel("count (log scale)")
right.set_title("How much the busiest symbols account for")
right.set_xlabel("busiest symbols (% of all symbols)")
right.set_ylabel("share of all events or trades (%)")
right.axvline(10, color="#9ca3af", linewidth=1, linestyle="--")   # marks the top 10%

for ax in (left, right):
    ax.grid(True, which="major", color="#e5e7eb", linewidth=0.8)
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False)

fig.suptitle("Mon 8 – Sun 14 Nov 2021: a few symbols carry most of the traffic")
fig.tight_layout()
fig.savefig("longtail-week.png", dpi=150)
print("saved longtail-week.png")