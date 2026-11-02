import streamlit as st
import pandas as pd
import json
from kafka import KafkaConsumer

# --------------------------------------------------
# Page Configuration
# --------------------------------------------------

st.set_page_config(
    page_title="Financial Tick Dashboard",
    layout="wide"
)

st.title("📈 Financial Tick Dashboard")

# --------------------------------------------------
# Kafka Consumer
# --------------------------------------------------

consumer = KafkaConsumer(
    "ema-results",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    consumer_timeout_ms=5000,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

records = []

for msg in consumer:
    records.append(msg.value)

# --------------------------------------------------
# No Data Case
# --------------------------------------------------

if len(records) == 0:
    st.warning("No data received from ema-results topic.")
    st.stop()

# --------------------------------------------------
# Convert to DataFrame
# --------------------------------------------------

df = pd.DataFrame(records)

# Convert timestamp to readable datetime
df["windowStart"] = pd.to_datetime(
    df["windowStart"],
    unit="ms"
)

# --------------------------------------------------
# KPI Section
# --------------------------------------------------

buy_count = len(df[df["signal"] == "buy"])
sell_count = len(df[df["signal"] == "sell"])

col1, col2, col3 = st.columns(3)

col1.metric("Total Records", len(df))
col2.metric("Buy Signals", buy_count)
col3.metric("Sell Signals", sell_count)

st.divider()

# --------------------------------------------------
# EMA Results Table
# --------------------------------------------------

st.subheader("Latest EMA Results")

st.dataframe(
    df,
    use_container_width=True
)

st.divider()

# --------------------------------------------------
# Trading Signals Table
# --------------------------------------------------

st.subheader("Trading Signals")

signals = df[df["signal"] != ""]

if len(signals) > 0:

    st.dataframe(
        signals[
            [
                "ID",
                "windowStart",
                "signal",
                "ema38",
                "ema100"
            ]
        ],
        use_container_width=True
    )

else:
    st.info("No trading signals available.")

st.divider()

# --------------------------------------------------
# EMA Chart by Symbol
# --------------------------------------------------

st.subheader("EMA Comparison")

symbols = sorted(df["ID"].unique())

selected_symbol = st.selectbox(
    "Select Symbol",
    symbols
)

symbol_df = (
    df[df["ID"] == selected_symbol]
    .sort_values("windowStart")
)

chart_df = symbol_df.set_index("windowStart")[
    ["ema38", "ema100"]
]

st.line_chart(chart_df)

# --------------------------------------------------
# Symbol Statistics
# --------------------------------------------------

st.subheader("Selected Symbol Summary")

latest = symbol_df.iloc[-1]

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Symbol",
    latest["ID"]
)

col2.metric(
    "EMA38",
    round(latest["ema38"], 2)
)

col3.metric(
    "EMA100",
    round(latest["ema100"], 2)
)

col4.metric(
    "Latest Signal",
    latest["signal"] if latest["signal"] else "None"
)