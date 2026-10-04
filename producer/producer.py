import pandas as pd
import json
from kafka import KafkaProducer

producer = KafkaProducer(
	bootstrap_servers="localhost:9092",
	value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

df = pd.read_csv("../analysis/monday_sample.csv",
	comment="#",
	usecols=["ID","Last","Trading time","Trading date"],
	index_col=False)

df = df[df["Last"].notna()]

for _, row in df.iterrows():
	event={
		"symbol":row["ID"],
		"price":row["Last"],
		"tradingTime":row["Trading time"],
		"tradingDate":row["Trading date"]
	}

	producer.send("trading-events", event)

	print("Sent:",event)

producer.flush()

print("Finished sending all events")
	
