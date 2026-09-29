import pandas as pd

PATH = "../data/debs2022-gc-trading-day-13-11-21.csv"

df = pd.read_csv(
    PATH,
    comment="#",       # skips every line starting with '#', wherever it appears
    index_col=False,   # tells pandas the extra trailing field is junk, not an index column
    low_memory=False,
)

# A few header names have a sneaky leading space (" Day low bid time"), so tidy them up
df.columns = df.columns.str.strip()

print(df.shape)

# Q1: how big is this day, really?
print("events:", len(df))
print("unique symbols:", df["ID"].nunique())

# Q2: split by exchange and security type
# The exchange is whatever sits after the last dot in ID, e.g. "SGFI.FR" -> "FR"
df["exchange"] = df["ID"].str.rsplit(".", n=1).str[-1]
print(df["exchange"].value_counts())
print(df["SecType"].value_counts())

# ---------- Q3: how many rows are real price events? ----------

has_last = df["Last"].notna()   # True wherever the Last column has any value at all
real_last = df["Last"] > 0      # True only where that value is an actual price

print("rows with a Last value:", has_last.sum())
print("rows with Last > 0:   ", real_last.sum())
print(f"that's {has_last.mean():.2%} vs {real_last.mean():.2%} of all events")

# Everything from here on works on the real price events only
prices = df[real_last].copy()


# ---------- Q4: do the trade dates and times look sane? ----------

# dropna=False so missing dates show up as NaN instead of quietly vanishing
print(prices["Trading date"].value_counts(dropna=False))

# How many "real" prices still carry the placeholder midnight time?
print("Trading time == 00:00:00.000:", (prices["Trading time"] == "00:00:00.000").sum())


# ---------- Q5: which symbols are the loudest? ----------

top_all = df["ID"].value_counts().head(10)         # all events, housekeeping included
top_prices = prices["ID"].value_counts().head(10)  # price events only

print("Top 10 by all events:\n", top_all)
print("Top 10 by price events:\n", top_prices)

overlap = set(top_all.index) & set(top_prices.index)
print(f"{len(overlap)} symbols appear in both lists:", overlap)