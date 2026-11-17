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

    streamlit run dashboard/dashboard.py      (from the repo root, so .streamlit/config.toml applies)
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

EXCHANGES = {"FR": "Paris", "NL": "Amsterdam", "ETR": "Frankfurt"}
WARMUP_WINDOWS = 100   # EMA100 needs about this many windows before it stops being dominated by EMA0 = 0

# One palette with separate steps for light and dark backgrounds (same hues, each set checked for
# contrast and colour-blind separation on its own background). Green and red are hard to tell apart
# for some colour-blind readers, so buy and sell always come with a shape (▲ / ▼) or a position
# (right or left of zero) as well as a colour.
LIGHT = dict(buy="#008300", sell="#e34948", price="#6b6a65", ema38="#2a78d6", ema100="#eb6834",
             surface="#fcfcfb", ink2="#52514e", muted="#898781", grid="#e9e8e3", axis="#c3c2b7")
DARK = dict(buy="#0ca30c", sell="#e66767", price="#a8a79f", ema38="#3987e5", ema100="#d95926",
            surface="#1a1a19", ink2="#c3c2b7", muted="#898781", grid="#2c2c2a", axis="#383835")

SERIES = {"close": "Close (last trade)", "ema38": "EMA38", "ema100": "EMA100"}
ADVISORY = {"buy": "Buy advisory", "sell": "Sell advisory"}
BULL, BEAR = "Bullish: EMA38 above EMA100", "Bearish: EMA38 below EMA100"

st.set_page_config(page_title="Trading trends", page_icon=":material/candlestick_chart:", layout="wide")

# A little depth for the cards. Borders and colours come from the theme in .streamlit/config.toml.
st.html("""
<style>
  div[class*="st-key-card"] { box-shadow: 0 1px 2px rgba(16,16,15,.04), 0 8px 24px -12px rgba(16,16,15,.10); }
  [data-testid="stMetric"] { box-shadow: 0 1px 2px rgba(16,16,15,.04); }
  [data-testid="stMetricLabel"] p { font-weight: 500; opacity: .8; }
  [data-testid="stMainBlockContainer"] { max-width: 1480px; }
</style>
""")


def colours() -> dict:
    return DARK if st.context.theme.type == "dark" else LIGHT


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


# ------------------------------------------------------------------ small helpers

def price(v: float) -> str:
    return f"{v:,.2f}" if abs(v) >= 100 else f"{v:,.4f}"


def day_label(d: pd.Timestamp) -> str:
    return f"{d:%a} {d.day} {d:%b}"


def short_day(d: pd.Timestamp) -> str:
    return f"{d:%a} {d.day}"


def as_datetime(t: pd.Timestamp) -> alt.DateTime:
    return alt.DateTime(year=t.year, month=t.month, date=t.day, hours=t.hour, minutes=t.minute)


def time_axis(windows: pd.Series):
    """X scale and axis for a run of windows: whole days for a multi-day view, hours within one day.

    Lining the week view up on midnights makes every trading day the same width, with a gridline
    between days, so the nights (no trading) read as gaps between sessions rather than as data.
    """
    first, last = windows.min(), windows.max()
    if first.normalize() != last.normalize():
        lo, hi = first.normalize(), last.normalize() + pd.Timedelta(hours=23, minutes=59)
        axis = alt.Axis(format="%a %-d %b", tickCount={"interval": "day", "step": 1}, labelAlign="left",
                        labelOffset=6, tickSize=0, grid=True)
    else:
        lo, hi = first.floor("h"), last.floor("h") + pd.Timedelta(hours=1)
        # A plain count here: Vega's renderer doesn't accept {"interval": "hour"}, even though the schema does
        axis = alt.Axis(format="%H:%M", tickCount=8, grid=False)
    return alt.Scale(domain=[as_datetime(lo), as_datetime(hi)]), axis


def styled(chart, c: dict):
    """The same quiet axes, grid and legends on every chart."""
    return (chart
            .configure_axis(labelColor=c["muted"], titleColor=c["ink2"], labelFontSize=12, titleFontSize=12,
                            titleFontWeight=500, gridColor=c["grid"], domainColor=c["axis"], tickColor=c["axis"],
                            labelPadding=6, titlePadding=10)
            .configure_legend(labelColor=c["ink2"], labelFontSize=12, labelLimit=0, symbolSize=110, columnPadding=18,
                              padding=0, offset=14)
            .configure_view(stroke=None))


def advisory_shapes(c: dict) -> tuple:
    """Buy is a green ▲ and sell a red ▼. Shape and colour share one legend, so it shows both."""
    lg = alt.Legend(orient="top", title=None, symbolSize=170)
    shape = alt.Shape("label:N", legend=lg, scale=alt.Scale(domain=list(ADVISORY.values()),
                                                            range=["triangle-up", "triangle-down"]))
    fill = alt.Fill("label:N", legend=lg, scale=alt.Scale(domain=list(ADVISORY.values()), range=[c["buy"], c["sell"]]))
    return shape, fill


# ------------------------------------------------------------------ charts

def trend_overview(ready: pd.DataFrame, c: dict):
    """Every symbol's current trend as a sorted bar: how far EMA38 sits above or below EMA100."""
    d = ready.assign(spread=(ready["ema38"] - ready["ema100"]) / ready["ema100"] * 100)
    d["trend"] = d["spread"].gt(0).map({True: BULL, False: BEAR})
    order = d.sort_values("spread", ascending=False)["symbol"].tolist()

    base = alt.Chart(d).encode(
        y=alt.Y("symbol:N", sort=order, title=None, scale=alt.Scale(paddingInner=0.32),
                axis=alt.Axis(labelOverlap=False, ticks=False, domain=False, labelColor=c["ink2"], labelPadding=8)),
        x=alt.X("spread:Q", title="EMA38 above (+) or below (−) EMA100, % of EMA100",
                axis=alt.Axis(tickCount=6, labelExpr="datum.value == 0 ? '0' : format(datum.value, '+.2~f')")),
        color=alt.Color("trend:N", scale=alt.Scale(domain=[BULL, BEAR], range=[c["buy"], c["sell"]]),
                        legend=alt.Legend(orient="top", title=None, symbolType="square")),
        tooltip=["symbol", alt.Tooltip("exchange", title="exchange"),
                 alt.Tooltip("window:T", title="latest window", format="%a %d %b %H:%M"),
                 alt.Tooltip("close:Q", format=",.4f"), alt.Tooltip("ema38:Q", format=",.4f", title="EMA38"),
                 alt.Tooltip("ema100:Q", format=",.4f", title="EMA100"),
                 alt.Tooltip("spread:Q", format="+.3f", title="spread (%)")],
    )
    # Round only the outer end of each bar, whichever side of zero it grows towards
    up = base.transform_filter("datum.spread > 0").mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
    down = base.transform_filter("datum.spread <= 0").mark_bar(cornerRadiusTopLeft=4, cornerRadiusBottomLeft=4)
    zero = alt.Chart(pd.DataFrame({"x": [0]})).mark_rule(color=c["axis"], strokeWidth=1).encode(x="x:Q")
    return styled((up + down + zero).properties(height=max(220, 24 * len(d))), c)


def symbol_chart(s: pd.DataFrame, c: dict, fit_price: bool, show_warmup_signals: bool):
    s = s.assign(day=s["window"].dt.strftime("%Y-%m-%d"))
    long = s.melt(id_vars=["window", "day"], value_vars=list(SERIES), var_name="series", value_name="value")
    long["series"] = long["series"].map(SERIES)

    lo, hi = s["close"].min(), s["close"].max()
    pad = (hi - lo) * 0.08 or abs(hi) * 0.01 or 1.0
    # EMAs start at 0, so on the price's own scale their warm-up comes up from below the chart and is cut off
    y_scale = alt.Scale(domain=[lo - pad, hi + pad]) if fit_price else alt.Scale(zero=False)
    x_scale, x_axis = time_axis(s["window"])
    x = alt.X("window:T", title=None, scale=x_scale, axis=x_axis)
    series_colour = alt.Color("series:N", scale=alt.Scale(domain=list(SERIES.values()),
                                                         range=[c["price"], c["ema38"], c["ema100"]]),
                              legend=alt.Legend(orient="top", title=None, symbolType="stroke", symbolStrokeWidth=3))

    hover = alt.selection_point(fields=["window"], nearest=True, on="pointerover", empty=False, clear="pointerout")
    lines = alt.Chart(long).mark_line(clip=True, interpolate="linear").encode(
        x=x, y=alt.Y("value:Q", title=None, scale=y_scale, axis=alt.Axis(tickCount=6)),
        color=series_colour,
        detail="day:N",   # one line per trading day, so nothing is drawn across the night
        strokeWidth=alt.condition(f"datum.series == '{SERIES['close']}'", alt.value(1.4), alt.value(2.2)),
    )
    rule = alt.Chart(s).mark_rule(color=c["muted"], strokeWidth=1).encode(
        x=x, opacity=alt.condition(hover, alt.value(0.7), alt.value(0)),
        tooltip=[alt.Tooltip("window:T", title="window", format="%a %d %b %H:%M"),
                 alt.Tooltip("close:Q", format=",.4f"), alt.Tooltip("ema38:Q", title="EMA38", format=",.4f"),
                 alt.Tooltip("ema100:Q", title="EMA100", format=",.4f")]
                + ([alt.Tooltip("trades:Q")] if "trades" in s else []),
    ).add_params(hover)
    dots = alt.Chart(long).mark_circle(size=50, opacity=1, stroke=c["surface"], strokeWidth=1.5, clip=True).encode(
        x=x, y=alt.Y("value:Q", scale=y_scale), color=series_colour
    ).transform_filter(hover)

    sig = s[s["signal"] != ""]
    if not show_warmup_signals:
        sig = sig[~sig["firstWindow"].astype(bool)]
    sig = sig.assign(label=sig["signal"].map(ADVISORY))
    shape, fill = advisory_shapes(c)
    markers = alt.Chart(sig).mark_point(size=190, filled=True, opacity=1, stroke=c["surface"], strokeWidth=1.5,
                                        clip=True).encode(
        x=x, y=alt.Y("close:Q", scale=y_scale), shape=shape, fill=fill,
        tooltip=[alt.Tooltip("window:T", title="window", format="%a %d %b %H:%M"),
                 alt.Tooltip("label:N", title="advisory"), alt.Tooltip("close:Q", format=",.4f"),
                 alt.Tooltip("ema38:Q", title="EMA38", format=",.4f"),
                 alt.Tooltip("ema100:Q", title="EMA100", format=",.4f")],
    )
    chart = (lines + rule + dots + markers).properties(height=380)
    return styled(chart.interactive(bind_y=False), c)


def advisory_timeline(adv: pd.DataFrame, all_windows: pd.Series, c: dict):
    d = adv.assign(label=adv["signal"].map(ADVISORY))
    order = (d[["exchange", "symbol"]].drop_duplicates().sort_values(["exchange", "symbol"])["symbol"].tolist())
    x_scale, x_axis = time_axis(all_windows)
    shape, fill = advisory_shapes(c)
    chart = alt.Chart(d).mark_point(size=85, filled=True, opacity=0.95, stroke=c["surface"], strokeWidth=1).encode(
        x=alt.X("window:T", title=None, scale=x_scale, axis=x_axis),
        y=alt.Y("symbol:N", title=None, sort=order,
                axis=alt.Axis(grid=True, ticks=False, domain=False, labelColor=c["ink2"], labelPadding=8)),
        shape=shape, fill=fill,
        tooltip=["symbol", alt.Tooltip("label:N", title="advisory"),
                 alt.Tooltip("window:T", title="window", format="%a %d %b %H:%M"),
                 alt.Tooltip("close:Q", format=",.4f")],
    ).properties(height=max(160, 26 * len(order)))
    return styled(chart, c)


# ------------------------------------------------------------------ page

def header(df: pd.DataFrame, source_badge: str):
    st.caption("CS-E4780 Scalable Systems and Data Management · DEBS 2022 Grand Challenge tick data")
    st.title("Trading trends", anchor=False)
    st.markdown("EMA38 and EMA100 on 5-minute windows for every subscribed symbol, and the buy and sell "
                "advisories that fire when the two cross.")
    badges = [source_badge]
    if not df.empty:
        days = df["window"].dt.normalize()
        span = day_label(days.min()) if days.min() == days.max() else f"{day_label(days.min())} – {day_label(days.max())}"
        badges.append(f":gray-badge[:material/calendar_month: {span} {days.max():%Y}]")
        badges.append(f":gray-badge[:material/stacks: {df['symbol'].nunique():,} symbols]")
    badges.append(":gray-badge[:material/timer: 5-minute tumbling windows]")
    st.markdown(" ".join(badges))


def card(key: str):
    return st.container(border=True, key=f"card_{key}")


def render(df: pd.DataFrame, source_badge: str, show_warmup_signals: bool):
    c = colours()
    header(df, source_badge)
    if df.empty:
        st.info("No results yet. Once the job is running and the producer is replaying data, windows appear "
                "here as they close.", icon=":material/hourglass_empty:")
        return

    present = [e for e in EXCHANGES if e in set(df["exchange"])]
    pick = st.segmented_control(
        "Exchange", ["all"] + present, default="all", key="exchange", label_visibility="collapsed",
        format_func=lambda e: "All exchanges" if e == "all" else f"{EXCHANGES[e]} ({e})")
    if pick and pick != "all":
        df = df[df["exchange"] == pick]

    adv = df[df["signal"] != ""]
    real_adv = adv if show_warmup_signals else adv[~adv["firstWindow"].astype(bool)]
    latest = df.sort_values("window").groupby("symbol").tail(1)
    latest = latest.merge(df.groupby("symbol").size().rename("windows"), on="symbol")
    ready = latest[latest["windows"] >= WARMUP_WINDOWS]

    buys, sells = real_adv[real_adv["signal"] == "buy"], real_adv[real_adv["signal"] == "sell"]
    bullish = int((ready["ema38"] > ready["ema100"]).sum())
    k = st.columns(5)
    k[0].metric("Windows", f"{len(df):,}", border=True,
                help="Symbol-windows with at least one trade. Each one is an EMA update.")
    k[1].metric("Buy advisories", f"{len(buys):,}", border=True, help="EMA38 crossed above EMA100")
    k[2].metric("Sell advisories", f"{len(sells):,}", border=True, help="EMA38 crossed below EMA100")
    k[3].metric("Bullish right now", f"{bullish} of {len(ready)}" if len(ready) else "–", border=True,
                help=f"Symbols whose EMA38 is above EMA100 in their latest window (only symbols with at least "
                     f"{WARMUP_WINDOWS} windows, so EMA100 has warmed up)")
    k[4].metric("Latest window", df["window"].max().strftime("%a %H:%M"), border=True,
                help="Start of the newest 5-minute window, in market time")

    table_height = max(300, 24 * len(ready) + 70)
    left, right = st.columns([2, 3], gap="medium")
    with left, card("feed"):
        st.subheader(":material/notifications: Latest advisories", anchor=False)
        hidden = len(adv) - len(real_adv)
        st.caption("Newest first." + (f" {hidden:,} first-window advisories are hidden: with EMA₀ = 0, every "
                                      "symbol's first window is a buy by definition." if hidden else ""))
        feed = real_adv.sort_values(["window", "symbol"], ascending=[False, True]).head(500)
        view = pd.DataFrame({"window": feed["window"], "symbol": feed["symbol"],
                             "advisory": feed["signal"].map({"buy": "▲ Buy", "sell": "▼ Sell"}),
                             "close": feed["close"]})
        tint = {"▲ Buy": c["buy"], "▼ Sell": c["sell"]}
        st.dataframe(view.style.map(lambda v: f"color: {tint.get(v, 'inherit')}; font-weight: 600",
                                    subset=["advisory"]),
                     hide_index=True, height=table_height, width="stretch",
                     column_config={"window": st.column_config.DatetimeColumn("Window", format="ddd HH:mm", width="small"),
                                    "symbol": st.column_config.TextColumn("Symbol"),
                                    "advisory": st.column_config.TextColumn("Advisory", width="small"),
                                    "close": st.column_config.NumberColumn("Close", format="%.4f")})
    with right, card("trend"):
        st.subheader(":material/query_stats: Where each symbol stands now", anchor=False)
        st.caption("How far the short-term average (EMA38) sits above or below the long-term one (EMA100) "
                   "in each symbol's latest window. Above zero is a buy trend, below is a sell trend.")
        if len(ready):
            st.altair_chart(trend_overview(ready, c), width="stretch")
        warming = sorted(latest.loc[latest["windows"] < WARMUP_WINDOWS, "symbol"])
        if warming:
            st.caption(f"Still warming up, so not shown (fewer than {WARMUP_WINDOWS} windows, and EMA100 is "
                       f"still climbing from 0): {', '.join(warming)}")

    with card("symbol"):
        st.subheader(":material/candlestick_chart: Why an advisory fired", anchor=False)
        counts = real_adv["symbol"].value_counts()
        symbols = sorted(df["symbol"].unique())
        # Start on the busiest symbol, and move there again when a filter hides the one that was picked
        if st.session_state.get("symbol") not in symbols:
            st.session_state["symbol"] = counts.index[0] if len(counts) else symbols[0]
        pick_col, day_col, fit_col = st.columns([3, 6, 2], vertical_alignment="bottom")
        sym = pick_col.selectbox("Symbol", symbols, key="symbol",
                                 format_func=lambda x: f"{x}   ·   {counts.get(x, 0)} advisories")
        s = df[df["symbol"] == sym].sort_values("window")
        sym_days = sorted(s["window"].dt.normalize().unique())
        day = None
        if len(sym_days) > 1:
            choice = day_col.segmented_control("Day", ["week"] + [short_day(d) for d in sym_days], default="week",
                                               format_func=lambda o: "Whole week" if o == "week" else o)
            day = next((d for d in sym_days if short_day(d) == choice), None)
        fit = fit_col.toggle("Fit to price", True,
                             help="EMAs start at 0. With this on, their climb from 0 runs off the bottom of the "
                                  "chart instead of squashing the price line.")
        shown = s if day is None else s[s["window"].dt.normalize() == day]
        st.altair_chart(symbol_chart(shown, c, fit, show_warmup_signals), width="stretch")

        last = s.iloc[-1]
        spread = (last["ema38"] - last["ema100"]) / last["ema100"] * 100 if last["ema100"] else 0.0
        trend = (f":green[**▲ {spread:+.2f}%**] EMA38 above EMA100" if spread > 0
                 else f":red[**▼ {spread:+.2f}%**] EMA38 below EMA100")
        mine = real_adv[real_adv["symbol"] == sym]["signal"].value_counts()
        st.markdown(f"**{sym}** ({EXCHANGES.get(last['exchange'], last['exchange'])}) · last close "
                    f"**{price(last['close'])}** · {trend} · {mine.get('buy', 0)} buy and {mine.get('sell', 0)} "
                    "sell advisories")
        st.caption("A buy advisory fires in the window where EMA38 rises above EMA100, a sell where it falls "
                   "below. Hover for values, drag to pan, scroll to zoom, double-click to reset.")

    if len(real_adv):
        with card("timeline"):
            st.subheader(":material/timeline: All advisories over time", anchor=False)
            st.caption(f"{len(real_adv):,} advisories across {real_adv['symbol'].nunique()} symbols, grouped by "
                       "exchange. Clusters show when the market turned.")
            st.altair_chart(advisory_timeline(real_adv, df["window"], c), width="stretch")

    with st.expander(f"Table view of {sym}", icon=":material/table_rows:"):
        st.dataframe(s.drop(columns=[x for x in ("maxIngestTs", "firedTs", "emitTs", "kafkaTs") if x in s]),
                     hide_index=True, width="stretch")


def main():
    with st.sidebar:
        st.subheader(":material/database: Data", anchor=False)
        source = st.radio("Data source", ["Live Kafka", "File"], horizontal=True, label_visibility="collapsed",
                          index=1 if os.environ.get("DASHBOARD_FILE") else 0)
        if source == "Live Kafka":
            bootstrap = st.text_input("Kafka bootstrap", BOOTSTRAP)
            topic = st.text_input("Topic", "ema-results")
            refresh = st.slider("Refresh every (s)", 2, 30, 5)
        else:
            path = st.text_input("Results file", os.environ.get("DASHBOARD_FILE",
                                                                str(REPO / "answer_key" / "answer-key-week.parquet")),
                                 help="A .jsonl dump of Flink's ema-results topic, or the answer key (.parquet)")
            subscribed_only = st.toggle("Subscribed symbols only", True, help="The symbols in config/subscriptions.txt")
        st.divider()
        st.subheader(":material/tune: Display", anchor=False)
        show_warmup = st.toggle("Show first-window advisories", False,
                                help="With EMA₀ = 0, every symbol's first window counts as a buy. "
                                     "They're hidden by default because they say nothing about the market.")
        st.divider()
        st.caption("Tick data from the DEBS 2022 Grand Challenge "
                   "([Zenodo 6382482](https://doi.org/10.5281/zenodo.6382482), CC BY-NC-SA 4.0): one week of "
                   "trades from Paris, Amsterdam and Frankfurt.")

    if source == "Live Kafka":
        @st.fragment(run_every=f"{refresh}s")
        def live():
            badge = f":green-badge[:material/sensors: Live from {topic}]"
            try:
                df = poll_kafka(bootstrap, topic)
            except Exception as e:  # broker not up yet, etc.
                header(pd.DataFrame(), badge)
                st.warning(f"Can't read {topic} from {bootstrap}: {e}", icon=":material/cloud_off:")
                return
            render(df, badge, show_warmup)
        live()
    else:
        badge = f":blue-badge[:material/description: {Path(path).name}]"
        if not Path(path).exists():
            header(pd.DataFrame(), badge)
            st.error(f"Can't find {path}. Pick a results file in the sidebar.", icon=":material/error:")
            return
        render(load_file(path, subscribed_only), badge, show_warmup)


main()
