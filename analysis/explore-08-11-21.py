import pandas as pd

prices = pd.read_parquet("../data/prices-08-11-21.parquet")
print(prices.dtypes)
print(f"{len(prices):,} trades across {prices['ID'].nunique():,} symbols")

# 1. Where do the trades come from? Exchange x security type, with totals
prices["exchange"] = prices["ID"].str.rsplit(".", n=1).str[-1]
print(pd.crosstab(prices["exchange"], prices["SecType"], margins=True))

# 2. Which day did these trades actually happen on?
#    Anything other than 08-11-2021 is a stale trade being rebroadcast
print(prices["Trading date"].value_counts(dropna=False).head(10))

# 3. Suspicious times: placeholder midnights, or missing times
print("trades at exactly 00:00:00.000:", (prices["Trading time"] == "00:00:00.000").sum())
print("trades with no trading time:   ", prices["Trading time"].isna().sum())

# 4. What does a trading day look like? Trades per hour (first two characters = the hour)
print(prices["Trading time"].str[:2].value_counts().sort_index())

# 5. Ties: the same symbol with two trades at the exact same millisecond
same_time = prices.duplicated(subset=["ID", "Trading date", "Trading time"], keep=False)
same_time_and_price = prices.duplicated(subset=["ID", "Trading date", "Trading time", "Last"], keep=False)
print("rows tied with another trade of the same symbol:", same_time.sum())
print("  ...of which also have the identical price:     ", same_time_and_price.sum())