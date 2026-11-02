import streamlit as st
import pandas as pd
import json
from kafka import KafkaConsumer

st.set_page_config(page_title="Trading Dashboard", layout="wide")

st.title("📈 Financial Tick Dashboard")

# Kafka consumer
consumer = KafkaConsumer(
    "ema-results",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    consumer_timeout_ms=5000,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

records = []

for message in consumer.poll(timeout_ms=3000).values():
    for msg in message:
        records.append(msg.value)

st.write("Records received:", len(records))

if records:

    df = pd.DataFrame(records)

    st.subheader("Latest EMA Results")
    st.dataframe(df)

    signals = df[df["signal"] != ""]

    st.subheader("Trading Signals")

    if len(signals) > 0:
        st.dataframe(signals[["ID", "signal", "ema38", "ema100"]])
    else:
        st.info("No signals available")

else:
    st.warning("No data received from ema-results topic")