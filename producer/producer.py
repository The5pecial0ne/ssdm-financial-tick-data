import pandas as pd

df = pd.read_csv("../analysis/monday_sample.csv",
	comment="#",
	usecols=["ID","Last","Trading time","Trading date"],
	index_col=False)

for _, row in df.iterrows():
	event={
		"symbol":row["ID"],
		"price":row["Last"],
		"tradingTime":row["Trading time"],
		"tradingDate":row["Trading date"]
	}

	print(event)
