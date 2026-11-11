"""
Live view of the pipeline's output.

    ema-results  ->  price and both averages per symbol        (Query 1)
    advisories   ->  buy / sell crossovers, newest first        (Query 2)

A background thread keeps reading the two Kafka topics into memory. The page
redraws itself every couple of seconds from whatever has arrived so far.

Run it through docker compose (http://localhost:8501), or on the host with
    streamlit run dashboard/dashboard.py
Set DASHBOARD_FILE to a JSON Lines file of results to look at a finished run
without Kafka.
"""

import json
import os
import threading
import time
from collections import defaultdict, deque

import altair as alt
import pandas as pd
import streamlit as st

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
RESULTS_FILE = os.environ.get("DASHBOARD_FILE")
WINDOWS_KEPT = 2000          # per symbol; a trading week is about 850 windows
BUY, SELL = "#1a9850", "#d73027"


class Store:
    """Everything received so far. Written by the reader thread, read by the page."""

    def __init__(self):
        self.lock = threading.Lock()
        self.windows = defaultdict(lambda: deque(maxlen=WINDOWS_KEPT))   # symbol -> EMA lines
        self.advisories = deque(maxlen=500)
        self.received = 0
        self.error = None

    def add(self, record, kafka_ts=None):
        record["latency_ms"] = (kafka_ts - record["detected_at"]) if kafka_ts else None
        with self.lock:
            self.received += 1
            self.windows[record["symbol"]].append(record)
            if record.get("signal") and not record.get("first_window"):
                self.advisories.append(record)

    def snapshot(self):
        with self.lock:
            return ({symbol: list(rows) for symbol, rows in self.windows.items()},
                    list(self.advisories), self.received, self.error)


def read_kafka(store):
    """
    Reads ema-results from the beginning, forever. Crossovers are picked out of
    the same lines (the advisories topic carries exactly those), so one topic
    is enough here.
    """
    from confluent_kafka import Consumer

    while True:
        try:
            consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"dashboard-{time.time()}",
                                 "auto.offset.reset": "earliest", "enable.auto.commit": False})
            consumer.subscribe(["ema-results"])
            while True:
                for msg in consumer.consume(num_messages=2000, timeout=1.0):
                    if msg.error():
                        store.error = str(msg.error())
                        continue
                    store.error = None
                    store.add(json.loads(msg.value()), msg.timestamp()[1])
        except Exception as problem:        # Kafka not up yet, or restarted: say so and try again
            store.error = str(problem)
            time.sleep(3)


def read_file(store):
    with open(RESULTS_FILE, encoding="utf-8") as f:
        for line in f:
            record = json.loads(line)
            if "ema38" in record:
                store.add(record, record.get("_kafka_ts"))


@st.cache_resource
def get_store():
    store = Store()
    if RESULTS_FILE:
        read_file(store)
    else:
        threading.Thread(target=read_kafka, args=(store,), daemon=True).start()
    return store


def overview_table(windows):
    """One row per symbol: where it stands now and how close the averages are to crossing."""
    rows = []
    for symbol, lines in windows.items():
        latest = lines[-1]
        crossings = [r for r in lines if r["signal"] and not r["first_window"]]
        gap = (latest["ema38"] - latest["ema100"]) / latest["ema100"] * 100 if latest["ema100"] else 0.0
        rows.append({
            "Symbol": symbol,
            "Type": "Index" if latest["SecType"] == "I" else "Equity",
            "Last close": latest["close"],
            "Trend": "Up (EMA38 above)" if gap > 0 else "Down (EMA38 below)",
            "Gap between averages %": round(gap, 3),
            "Last advisory": crossings[-1]["signal"].upper() if crossings else "",
            "At": crossings[-1]["window_start"].replace("T", " ") if crossings else "",
            "Updated": latest["window_start"].replace("T", " "),
        })
    return pd.DataFrame(rows)


def symbol_chart(lines, hide_warmup):
    df = pd.DataFrame(lines)
    df["time"] = pd.to_datetime(df["window_start"])
    if hide_warmup:
        # Both averages start at 0, so early on they are still climbing towards the price.
        warmed_up = df[df["ema100"] > 0.9 * df["close"]]
        df = warmed_up if len(warmed_up) > 1 else df
    long = df.melt(id_vars="time", value_vars=["close", "ema38", "ema100"], var_name="line", value_name="price")
    long["line"] = long["line"].map({"close": "Window close", "ema38": "EMA38 (fast)", "ema100": "EMA100 (slow)"})

    x = alt.X("time:T", title="window start (exchange time, CET)")
    lines_layer = alt.Chart(long).mark_line().encode(
        x=x,
        y=alt.Y("price:Q", title="price", scale=alt.Scale(zero=False)),
        color=alt.Color("line:N", title=None,
                        scale=alt.Scale(domain=["Window close", "EMA38 (fast)", "EMA100 (slow)"],
                                        range=["#b0b0b0", "#2c7fb8", "#f28e2b"])),
        strokeWidth=alt.condition(alt.datum.line == "Window close", alt.value(1), alt.value(2.5)),
    )
    crossings = df[(df["signal"] != "") & ~df["first_window"]].copy()
    crossings["advisory"] = crossings["signal"].str.upper()
    marks = alt.Chart(crossings).mark_point(size=260, filled=True, opacity=1).encode(
        x=x, y="ema38:Q",
        shape=alt.Shape("advisory:N", title="Advisory",
                        scale=alt.Scale(domain=["BUY", "SELL"], range=["triangle-up", "triangle-down"])),
        fill=alt.Fill("advisory:N", legend=None, scale=alt.Scale(domain=["BUY", "SELL"], range=[BUY, SELL])),
        tooltip=[alt.Tooltip("advisory:N"), alt.Tooltip("time:T", format="%d %b %H:%M"),
                 alt.Tooltip("close:Q"), alt.Tooltip("ema38:Q", format=".4f"), alt.Tooltip("ema100:Q", format=".4f")],
    )
    return (lines_layer + marks).properties(height=380).interactive(bind_y=False)


st.set_page_config(page_title="Trading trends", layout="wide")
st.title("Trading trends: EMA crossovers on live tick data")
store = get_store()


@st.fragment(run_every=None if RESULTS_FILE else "2s")
def page():
    windows, advisories, received, error = store.snapshot()
    if error:
        st.warning(f"Kafka: {error}")
    if not windows:
        st.info("Waiting for the first 5-minute window to close. Results appear as soon as the Flink job publishes them.")
        return

    latencies = [a["latency_ms"] for a in advisories if a["latency_ms"] is not None]
    newest = max(lines[-1]["window_start"] for lines in windows.values())
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Subscribed symbols seen", len(windows))
    c2.metric("Window updates", f"{received:,}")
    c3.metric("Buy advisories", sum(a["signal"] == "buy" for a in advisories))
    c4.metric("Sell advisories", sum(a["signal"] == "sell" for a in advisories))
    c5.metric("Advisory latency (median)", f"{pd.Series(latencies).median():.0f} ms" if latencies else "n/a",
              help="From the moment Flink detects the crossover to the moment Kafka stores the advisory.")
    st.caption(f"Newest window: {newest.replace('T', ' ')} (exchange time). "
               "A symbol's very first window is not counted as a buy: both averages start at 0 there.")

    left, right = st.columns([2, 1])

    with right:
        st.subheader("Latest advisories")
        if advisories:
            feed = pd.DataFrame(reversed(advisories))
            feed = pd.DataFrame({
                "Window": feed["window_start"].str.replace("T", " "),
                "Symbol": feed["symbol"],
                "Advice": feed["signal"].str.upper(),
                "Close": feed["close"],
                "Latency ms": feed["latency_ms"],
            })
            colour = lambda v: f"color: white; background-color: {BUY if v == 'BUY' else SELL}"
            st.dataframe(feed.style.map(colour, subset=["Advice"]), hide_index=True, height=420,
                         width="stretch")
        else:
            st.write("No crossover yet. The slow average needs a few hours of windows to catch up with the price.")

    with left:
        st.subheader("One symbol")
        with_advice = {a["symbol"] for a in advisories}
        symbols = sorted(windows, key=lambda s: (s not in with_advice, s))
        label = lambda s: f"{s}  (has advisories)" if s in with_advice else s
        symbol = st.selectbox("Symbol", symbols, format_func=label, key="symbol")
        hide_warmup = st.checkbox("Hide the warm-up (averages still climbing from 0)", value=False, key="warmup")
        st.altair_chart(symbol_chart(windows[symbol], hide_warmup), width="stretch")

    st.subheader("All subscribed symbols")
    st.caption("Sorted by how close the two averages are: the top rows are the next likely crossovers.")
    table = overview_table(windows)
    table = table.sort_values("Gap between averages %", key=lambda gap: gap.abs())
    st.dataframe(table, hide_index=True, width="stretch")


page()
