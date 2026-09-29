import pandas as pd

PATH = "../data/debs2022-gc-trading-day-08-11-21.csv"
COLS = ["ID", "SecType", "Date", "Time", "Last", "Trading time", "Trading date"]

reader = pd.read_csv(
    PATH, comment="#", index_col=False, usecols=COLS,
    # Telling pandas the types up front means it never has to guess (no DtypeWarning)
    dtype={"Last": "float64", "Date": "string", "Time": "string",
           "Trading time": "string", "Trading date": "string"},
    chunksize=2_000_000,
)

chunks, total_rows = [], 0
for chunk in reader:
    chunk["seq"] = range(total_rows, total_rows + len(chunk))
    total_rows += len(chunk)
    chunks.append(chunk[chunk["Last"] > 0])

prices = pd.concat(chunks, ignore_index=True)
prices.to_parquet("../data/prices-08-11-21.parquet", index=False)  # overwrite, now with Date and Time

# A. Is the system date always Monday?
print(prices["Date"].value_counts(dropna=False))

# B. Which trades get a Trading date, and which don't?
print(pd.crosstab(prices["SecType"], prices["Trading date"].notna(), margins=True))

# C. A trade should never happen AFTER the system received it.
#    Plain string comparison works because both are zero-padded "HH:MM:SS.sss"
after = prices["Trading time"] > prices["Time"]
print("trades timed after their receive time:", after.sum())

# D. How long between the trade and the moment the system received it?
lag = pd.to_timedelta(prices["Time"]) - pd.to_timedelta(prices["Trading time"])
print(lag.describe())