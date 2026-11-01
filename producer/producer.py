import pandas as pd
import json
from kafka import KafkaProducer

producer = KafkaProducer(
	bootstrap_servers="localhost:9092",
	value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

df = pd.read_csv("../analysis/test-slice.csv",
	comment="#",
	usecols=["ID","SecType","Last","Trading time","Trading date"],
	index_col=False)
print(len(df),"rows read from CSV file")

print(df.columns.tolist())
print(df.head())
df["Trading date"] = "08-11-2021"

df = df[df["Last"].notna()
        & df["Trading time"].notna()]

print(len(df),"rows after filtering")

for _, row in df.iterrows():
	event={
		"symbol":row["ID"],
		"SecType":row["SecType"],
		"price":row["Last"],
		"tradingTime":row["Trading time"],
		"tradingDate":row["Trading date"]
	}

	producer.send("trading-events", event)

	print("Sent:",event)

producer.flush()

print("Finished sending all events")
	
