"""
Reading the DEBS 2022 day files, shared by the producer and the reference (answer key) code
so both see exactly the same events, filtered the same way.

The raw files are big (40-60 million rows a day), so they're read in chunks and only the
columns the project uses are kept.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("SSDM_DATA_DIR", REPO / "data"))

COLS = ["ID", "SecType", "Date", "Time", "Last", "Trading time"]
PLACEHOLDER = "01:00:00.000"          # fake trading time on some trades (midnight UTC shown in CET)
WEEKDAYS = ["08-11-21", "09-11-21", "10-11-21", "11-11-21", "12-11-21"]
WEEK = WEEKDAYS + ["13-11-21", "14-11-21"]
WINDOW_MS = 5 * 60 * 1000


def day_file(day: str, data_dir: Path = DATA_DIR) -> Path:
    return Path(data_dir) / f"debs2022-gc-trading-day-{day}.csv"


def resolve_days(spec: str) -> list[str]:
    """'week' -> all 7 days, 'weekdays' -> Mon-Fri, otherwise a comma-separated list like 08-11-21,09-11-21."""
    if spec == "week":
        return WEEK
    if spec == "weekdays":
        return WEEKDAYS
    return [d.strip() for d in spec.split(",") if d.strip()]


def read_chunks(path: Path, chunksize: int = 1_000_000, seq_offset: int = 0) -> Iterator[pd.DataFrame]:
    """
    Yields chunks of one day file with an extra 'seq' column (position in the file),
    which is the tie-break for trades with identical timestamps. 'Last' stays text so the
    exact price string is passed on; 'price' holds it as a number.
    """
    reader = pd.read_csv(path, comment="#", index_col=False, usecols=COLS, dtype=str,
                         chunksize=chunksize, keep_default_na=False)
    pos = seq_offset
    for chunk in reader:
        chunk["seq"] = np.arange(pos, pos + len(chunk), dtype=np.int64)
        pos += len(chunk)
        chunk["price"] = pd.to_numeric(chunk["Last"], errors="coerce").fillna(0.0)
        yield chunk


def apply_filters(df: pd.DataFrame, events: str = "trades", exchanges: list[str] | None = None,
                  t_from: str | None = None, t_to: str | None = None) -> pd.DataFrame:
    """
    events='trades' keeps rows with a positive Last price (placeholders included, so the job
    still sees and counts them); events='all' keeps quotes and housekeeping events too.
    exchanges filters on the ID suffix (FR, NL, ETR). t_from/t_to ('HH:MM') keep a time-of-day
    range based on Trading time, or receive Time when there is no trading time.
    """
    mask = np.ones(len(df), dtype=bool)
    if events == "trades":
        mask &= (df["price"] > 0).to_numpy()
    if exchanges:
        mask &= df["ID"].str.rsplit(".", n=1).str[-1].isin(exchanges).to_numpy()
    if t_from or t_to:
        t = df["Trading time"].where(df["Trading time"] != "", df["Time"])
        if t_from:
            mask &= (t >= t_from).to_numpy()
        if t_to:
            mask &= (t < t_to).to_numpy()
    return df[mask]


def trade_timestamps_ms(df: pd.DataFrame) -> np.ndarray:
    """Event time in ms (Date + Trading time read as UTC). NaN-like (-1) for non-trades and placeholders."""
    ok = (df["price"] > 0) & (df["Trading time"] != "") & (df["Trading time"] != PLACEHOLDER)
    out = np.full(len(df), -1, dtype=np.int64)
    if ok.any():
        sub = df.loc[ok]
        ts = pd.to_datetime(sub["Date"], format="%d-%m-%Y") + pd.to_timedelta(sub["Trading time"])
        out[ok.to_numpy()] = ts.astype("datetime64[ms]").astype(np.int64).to_numpy()
    return out
