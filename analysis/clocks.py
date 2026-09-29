import pandas as pd

prices = pd.read_parquet("../data/prices-08-11-21.parquet")   # rows are still in file order

# E. Is the receive Time only precise to the whole second?
print("Time ends in .000:        ", f"{prices['Time'].str.endswith('.000').mean():.1%}")
print("Trading time ends in .000:", f"{prices['Trading time'].str.endswith('.000').mean():.1%}")

# F. How many trades have a big gap between the two clocks, and what do the worst look like?
prices["tt"] = pd.to_timedelta(prices["Trading time"])
lag_s = (pd.to_timedelta(prices["Time"]) - prices["tt"]).dt.total_seconds()
for limit in [1, 5, 60, 300, 3600]:
    print(f"clocks more than {limit:>4} s apart:", (lag_s.abs() > limit).sum())

cols = ["ID", "SecType", "Time", "Trading time", "Last"]
print(prices.loc[lag_s.nsmallest(5).index, cols])   # trades stamped longest after receipt

# G. Within one symbol, does Trading time ever go backwards in file order?
#    cummax() keeps a running "latest trading time seen so far" per symbol
latest_so_far = prices.groupby("ID")["tt"].cummax()
behind_s = (latest_so_far - prices["tt"]).dt.total_seconds()
print("trades older than one already seen for the same symbol:", (behind_s > 0).sum())
print(behind_s[behind_s > 0].describe())

# The late trades that actually matter: their 5-minute window has already been passed
window = prices["tt"] // pd.Timedelta(minutes=5)
latest_window = latest_so_far // pd.Timedelta(minutes=5)
print("trades arriving after their window was passed:", (window < latest_window).sum())

# H. Is Trading time ordered across the WHOLE file, not just within each symbol?
#    One Flink watermark watches all symbols at once, so this is the order it sees
steps_back = prices["tt"].diff().dt.total_seconds()   # gap to the row before, any symbol
print("\nplaces where Trading time goes backwards:", (steps_back < 0).sum())
print("biggest step backwards (s):", -steps_back.min())
print(steps_back[steps_back < 0].describe())

# I. Profile the 1,070 odd trades
odd = prices[lag_s.abs() > 1]
print(odd["ID"].str.rsplit(".", n=1).str[-1].value_counts())      # which exchange
print(odd["SecType"].value_counts())                               # equity or index
print(odd["Trading time"].str[:5].value_counts().head(10))         # HH:MM of the stamped trade
print(odd["Time"].str[:2].value_counts().sort_index())             # hour they were received
print(odd["Trading date"].value_counts(dropna=False))
print("Time with real milliseconds, odd rows: ", (~odd["Time"].str.endswith(".000")).sum())
print("Time with real milliseconds, all rows: ", (~prices["Time"].str.endswith(".000")).sum())