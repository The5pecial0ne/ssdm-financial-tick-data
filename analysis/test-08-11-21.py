import time
import pandas as pd

PATH = "../data/debs2022-gc-trading-day-08-11-21.csv"

# Only the columns the answer key needs. The other 34 never get loaded.
COLS = ["ID", "SecType", "Last", "Trading time", "Trading date"]

reader = pd.read_csv(
    PATH,
    comment="#",
    index_col=False,
    usecols=COLS,
    dtype={"Last": "float64"},
    chunksize=2_000_000,   # read 2M rows at a time, then forget them
)

all_counts = pd.Series(dtype="int64")  # events per symbol, ALL event types (for the long-tail plot)
price_chunks = []                      # real trades only (for the answer key)
total_rows = 0
t0 = time.time()

for i, chunk in enumerate(reader):
    # Remember each row's position in the file. When two trades share the exact
    # same timestamp, this is how we'll decide which one came "last".
    chunk["seq"] = range(total_rows, total_rows + len(chunk))
    total_rows += len(chunk)

    # Tally every event per symbol, then add it onto the running total
    all_counts = all_counts.add(chunk["ID"].value_counts(), fill_value=0)

    # Keep only real trades and throw the rest away
    price_chunks.append(chunk[chunk["Last"] > 0])

    print(f"chunk {i}: {total_rows:,} rows, {time.time() - t0:.0f}s")

prices = pd.concat(price_chunks, ignore_index=True)
all_counts = all_counts.astype("int64").sort_values(ascending=False)

print(f"\ntotal events: {total_rows:,}")
print(f"real price events: {len(prices):,}")
print(f"symbols seen: {len(all_counts):,}")

# Save to Parquet so we never have to parse this CSV again
prices.to_parquet("../data/prices-08-11-21.parquet", index=False)
all_counts.rename("events").to_frame().to_parquet("../data/counts-08-11-21.parquet")