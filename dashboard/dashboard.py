"""
Trading-trends dashboard (the "smart visualization", spec section 3.4).

The idea: a trader shouldn't have to read EMA numbers. The page answers three questions,
in the order a trader asks them:
  1. What just happened?        -> the advisory feed, newest first
  2. Where is the market now?   -> every subscribed symbol's trend, as one sorted chart
  3. Why did it fire?           -> one symbol's price, EMA38, EMA100 and its buy/sell markers

Data sources (sidebar):
  Live Kafka   reads ema-results as the job publishes them (refreshes every few seconds)
  File         a JSON-lines dump of Flink's output, or the pandas answer key (.parquet),
               so the page also works without the pipeline running

    streamlit run dashboard/dashboard.py
"""
from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

REPO = Path(__file__).resolve().parent.parent
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

# Buy/sell always comes with a shape (triangle up/down) as well as a colour, so it reads without colour too
BUY, SELL = "#008300", "#e34948"
PRICE, EMA38, EMA100 = "#52514e", "#2a78d6", "#eb6834"

st.set_page_config(page_title="Trading trends", page_icon="📈", layout="wide")


# ------------------------------------------------------------------ data

def normalise(df: pd.DataFrame) -> pd.DataFrame:
    """Accepts Flink output (symbol/windowStart ms) or the answer key (ID/window_start)."""
    df = df.rename(columns={"ID": "symbol", "SecType": "secType", "first_window": "firstWindow"})
    if "windowStart" in df:
        df["window"] = pd.to_datetime(df["windowStart"], unit="ms")
    else:
        df["window"] = pd.to_datetime(df["window_start"])
    df["signal"] = df["signal"].fillna("")
    df["exchange"] = df["symbol"].str.rsplit(".", n=1).str[-1]
    return df


@st.cache_data(show_spinner="Loading results ...")
def load_file(path: str, subscribed_only: bool) -> pd.DataFrame:
    p = Path(path)
    df = pd.read_parquet(p) if p.suffix == ".parquet" else pd.read_json(p, lines=True)
    df = normalise(df)
    if subscribed_only:
        subs = read_subscriptions()
        if subs:
            df = df[df["symbol"].isin(subs)]
    return df


def read_subscriptions() -> list[str]:
    f = REPO / "config" / "subscriptions.txt"
    if not f.exists():
        return []
    return [l.strip() for l in f.read_text().splitlines() if l.strip() and not l.startswith("#")]


@st.cache_resource
def kafka_consumer(bootstrap: str, topic: str):
    from confluent_kafka import Consumer
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"dashboard-{uuid.uuid4()}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    return c


def poll_kafka(bootstrap: str, topic: str) -> pd.DataFrame:
    """Adds whatever arrived since the last refresh to the session's records."""
    c = kafka_consumer(bootstrap, topic)
    rows = st.session_state.setdefault("rows", [])
    deadline = time.time() + 1.5
    while time.time() < deadline:
        msgs = c.consume(5000, timeout=0.3)
        if not msgs:
            break
        rows.extend(json.loads(m.value()) for m in msgs if not m.error())
    return normalise(pd.DataFrame(rows)) if rows else pd.DataFrame()


# ------------------------------------------------------------------ charts

WARMUP_WINDOWS = 100   # EMA100 needs about this many windows before it stops being dominated by EMA0 = 0


def trend_overview(latest: pd.DataFrame):
    """Every symbol's current trend as a sorted bar: how far EMA38 sits above or below EMA100."""
    d = latest[latest["windows"] >= WARMUP_WINDOWS]
    d = d.assign(spread=(latest["ema38"] - latest["ema100"]) / latest["ema100"] * 100,
                      trend=lambda x: x["spread"].gt(0).map({True: "bullish (EMA38 above)",
                                                             False: "bearish (EMA38 below)"}))
    height = max(180, 22 * len(d))
    return (alt.Chart(d).mark_bar(cornerRadiusEnd=3).encode(
        y=alt.Y("symbol:N", sort="-x", title=None, axis=alt.Axis(labelOverlap=False)),
        x=alt.X("spread:Q", title="EMA38 vs EMA100 (%)"),
        color=alt.Color("trend:N", scale=alt.Scale(domain=["bullish (EMA38 above)", "bearish (EMA38 below)"],
                                                   range=[BUY, SELL]), legend=alt.Legend(orient="top", title=None)),
        tooltip=["symbol", "exchange", alt.Tooltip("window:T", title="last window", format="%a %d %b %H:%M"),
                 alt.Tooltip("close:Q", format=",.4f"), alt.Tooltip("ema38:Q", format=",.4f"),
                 alt.Tooltip("ema100:Q", format=",.4f"), alt.Tooltip("spread:Q", format="+.2f", title="spread %")],
    ).properties(height=height))


def symbol_chart(s: pd.DataFrame, zoom_to_price: bool, show_warmup_signals: bool):
    long = s.melt(id_vars=["window"], value_vars=["close", "ema38", "ema100"], var_name="series", value_name="value")
    long["series"] = long["series"].map({"close": "close (last trade)", "ema38": "EMA38", "ema100": "EMA100"})
    lo, hi = s["close"].min(), s["close"].max()
    pad = (hi - lo) * 0.08 or hi * 0.01
    yscale = alt.Scale(domain=[lo - pad, hi + pad], clamp=True) if zoom_to_price else alt.Scale(zero=False)

    hover = alt.selection_point(fields=["window"], nearest=True, on="pointerover", empty=False)
    lines = alt.Chart(long).mark_line(strokeWidth=2).encode(
        x=alt.X("window:T", title=None),
        y=alt.Y("value:Q", title="price", scale=yscale),
        color=alt.Color("series:N", scale=alt.Scale(domain=["close (last trade)", "EMA38", "EMA100"],
                                                    range=[PRICE, EMA38, EMA100]),
                        legend=alt.Legend(orient="top", title=None)),
        strokeDash=alt.condition("datum.series == 'close (last trade)'", alt.value([3, 2]), alt.value([1, 0])),
    )
    sig = s[s["signal"] != ""]
    if not show_warmup_signals:
        sig = sig[~sig["firstWindow"].astype(bool)]
    sig = sig.assign(label=sig["signal"].map({"buy": "buy advisory", "sell": "sell advisory"}))
    markers = alt.Chart(sig).mark_point(size=160, filled=True, stroke="white", strokeWidth=1.5, opacity=1).encode(
        x="window:T", y=alt.Y("close:Q", scale=yscale),
        shape=alt.Shape("label:N", scale=alt.Scale(domain=["buy advisory", "sell advisory"],
                                                   range=["triangle-up", "triangle-down"]), legend=None),
        fill=alt.Fill("label:N", scale=alt.Scale(domain=["buy advisory", "sell advisory"], range=[BUY, SELL]),
                      legend=alt.Legend(orient="top", title=None)),
        tooltip=[alt.Tooltip("window:T", format="%a %d %b %H:%M"), "label", alt.Tooltip("close:Q", format=",.4f"),
                 alt.Tooltip("ema38:Q", format=",.4f"), alt.Tooltip("ema100:Q", format=",.4f")],
    )
    rule = alt.Chart(s).mark_rule(color="#8a8984").encode(
        x="window:T", opacity=alt.condition(hover, alt.value(0.6), alt.value(0)),
        tooltip=[alt.Tooltip("window:T", format="%a %d %b %H:%M"), alt.Tooltip("close:Q", format=",.4f"),
                 alt.Tooltip("ema38:Q", format=",.4f"), alt.Tooltip("ema100:Q", format=",.4f"),
                 alt.Tooltip("trades:Q") if "trades" in s else alt.Tooltip("signal:N")],
    ).add_params(hover)
    return (lines + rule + markers).properties(height=360).interactive(bind_y=False)


def advisory_timeline(adv: pd.DataFrame):
    d = adv.assign(label=adv["signal"].map({"buy": "buy advisory", "sell": "sell advisory"}))
    return alt.Chart(d).mark_point(size=70, filled=True, opacity=0.9).encode(
        x=alt.X("window:T", title=None),
        y=alt.Y("symbol:N", title=None, sort=alt.SortField("symbol")),
        shape=alt.Shape("label:N", scale=alt.Scale(domain=["buy advisory", "sell advisory"],
                                                   range=["triangle-up", "triangle-down"]), legend=None),
        fill=alt.Fill("label:N", scale=alt.Scale(domain=["buy advisory", "sell advisory"], range=[BUY, SELL]),
                      legend=alt.Legend(orient="top", title=None)),
        tooltip=["symbol", "label", alt.Tooltip("window:T", format="%a %d %b %H:%M"),
                 alt.Tooltip("close:Q", format=",.4f")],
    ).properties(height=max(160, 16 * d["symbol"].nunique()))


# ------------------------------------------------------------------ page

def render(df: pd.DataFrame, show_warmup_signals: bool, zoom: bool):
    if df.empty:
        st.info("No results yet. Is the job running and the producer replaying data?")
        return
    adv = df[df["signal"] != ""]
    real_adv = adv if show_warmup_signals else adv[~adv["firstWindow"].astype(bool)]
    latest = df.sort_values("window").groupby("symbol").tail(1)
    latest = latest.merge(df.groupby("symbol").size().rename("windows"), on="symbol")

    k = st.columns(5)
    k[0].metric("Symbols", f"{df['symbol'].nunique():,}")
    k[1].metric("Windows evaluated", f"{len(df):,}")
    k[2].metric("Buy advisories", f"{(real_adv['signal'] == 'buy').sum():,}")
    k[3].metric("Sell advisories", f"{(real_adv['signal'] == 'sell').sum():,}")
    k[4].metric("Market time", df["window"].max().strftime("%a %H:%M"))
    if not show_warmup_signals:
        st.caption(f"{len(adv) - len(real_adv):,} first-window advisories hidden: with EMA₀ = 0 every symbol's "
                   "first window is a 'buy' by definition (toggle in the sidebar).")

    left, right = st.columns([2, 3], gap="large")
    with left:
        st.subheader("Latest advisories")
        feed = real_adv.sort_values("window", ascending=False).head(200)
        st.dataframe(feed.assign(advisory=feed["signal"].map({"buy": "▲ buy", "sell": "▼ sell"}))
                     [["window", "symbol", "advisory", "close", "ema38", "ema100"]],
                     hide_index=True, height=420,
                     column_config={"window": st.column_config.DatetimeColumn("window", format="ddd D MMM HH:mm"),
                                    "close": st.column_config.NumberColumn(format="%.4f"),
                                    "ema38": st.column_config.NumberColumn("EMA38", format="%.4f"),
                                    "ema100": st.column_config.NumberColumn("EMA100", format="%.4f")})
    with right:
        st.subheader("Where each symbol stands now")
        st.altair_chart(trend_overview(latest), width="stretch")
        warming = latest.loc[latest["windows"] < WARMUP_WINDOWS, "symbol"].tolist()
        if warming:
            st.caption(f"Not shown, still warming up (fewer than {WARMUP_WINDOWS} windows, so EMA100 is still "
                       f"climbing from 0): {', '.join(sorted(warming))}")

    st.subheader("Why an advisory fired")
    symbols = sorted(df["symbol"].unique())
    default = real_adv["symbol"].value_counts().index[0] if len(real_adv) else symbols[0]
    sym = st.selectbox("Symbol", symbols, index=symbols.index(default))
    s = df[df["symbol"] == sym].sort_values("window")
    st.altair_chart(symbol_chart(s, zoom, show_warmup_signals), width="stretch")
    st.caption("A buy advisory fires in the window where EMA38 rises above EMA100, a sell where it falls below. "
               "Drag to pan, scroll to zoom.")

    if len(real_adv):
        st.subheader("All advisories over time")
        st.altair_chart(advisory_timeline(real_adv), width="stretch")

    with st.expander("Table view of the selected symbol"):
        st.dataframe(s.drop(columns=[c for c in ("maxIngestTs", "firedTs", "emitTs", "kafkaTs") if c in s]),
                     hide_index=True)


def main():
    st.title("📈 Trading trends")
    st.caption("EMA38/EMA100 over 5-minute windows and the buy/sell advisories they trigger (DEBS 2022 tick data).")

    with st.sidebar:
        source = st.radio("Data source", ["Live Kafka", "File"], index=1 if os.environ.get("DASHBOARD_FILE") else 0)
        if source == "Live Kafka":
            bootstrap = st.text_input("Kafka bootstrap", BOOTSTRAP)
            topic = st.text_input("Topic", "ema-results")
            refresh = st.slider("Refresh every (s)", 2, 30, 5)
        else:
            path = st.text_input("Results file (.jsonl from Flink, or the answer key .parquet)",
                                 os.environ.get("DASHBOARD_FILE", str(REPO / "answer_key" / "answer-key-week.parquet")))
            subscribed_only = st.checkbox("Subscribed symbols only (config/subscriptions.txt)", True)
        st.divider()
        show_warmup = st.checkbox("Show first-window advisories", False)
        zoom = st.checkbox("Zoom price chart to the price range", True,
                           help="EMAs start at 0, so their warm-up would squash the price line otherwise.")

    if source == "Live Kafka":
        @st.fragment(run_every=f"{refresh}s")
        def live():
            try:
                render(poll_kafka(bootstrap, topic), show_warmup, zoom)
            except Exception as e:  # broker not up yet, etc.
                st.warning(f"Can't read {topic} from {bootstrap}: {e}")
        live()
    else:
        if not Path(path).exists():
            st.error(f"{path} not found")
            return
        render(load_file(path, subscribed_only), show_warmup, zoom)


main()
