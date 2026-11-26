"""
Build a small test dataset for the Flink job, plus the answer-key rows it
should reproduce.

Run from analysis/, like the other scripts:

    python make_test_slice.py --header-from path/to/any_raw_trading_csv.csv

Why this works: a symbol's EMAs only ever depend on that symbol's own trades.
So if we keep *every* trade of a symbol from the start of the week up to some
cutoff, the full-week answer key is still exactly right for those windows.
No recomputation, nothing new to trust.

What goes in:
  1. A169S8.ETR, Monday's first 6 windows. Busiest symbol, and the one we
     checked by hand (first window: EMA38 821.928718, EMA100 317.378416).
  2. One symbol with a real crossover on Tuesday, with all its Monday and
     Tuesday trades up to two windows after the signal. That tests windows
     that fire, gaps with no trades, carrying the EMAs over midnight, and a
     real buy or sell. We prefer A0S290.ETR (the near-tie case) if it fits
     the row budget, otherwise the cheapest symbol that has one.

What comes out (in ../data/test-slice/):
  test-slice.csv            raw-format CSV your Flink job can read as-is
  test-slice-expected.jsonl the answer-key rows, in the agreed JSON format
  test-slice-expected.csv   the same rows, easier to eyeball
"""
import argparse
import json
import os

import pandas as pd

DATA = "../data"
MON, TUE = "08-11-21", "09-11-21"
PLACEHOLDER = "01:00:00.000"      # the fake Wednesday timestamps (challenge 6)
HAND_CHECKED = "A169S8.ETR"
NEAR_TIE = "A0S290.ETR"
FIVE_MIN = pd.Timedelta(minutes=5)

ap = argparse.ArgumentParser()
ap.add_argument("--header-from", required=True,
                help="any raw trading CSV; only its header lines are copied")
ap.add_argument("--budget", type=int, default=5000, help="max trade rows in total")
ap.add_argument("--out", default=f"{DATA}/test-slice")
args = ap.parse_args()


def trades(day, symbols=None):
    # Same cleaning as the answer key: trades only (already true in the
    # parquet), fake 01:00 stamps dropped, event time = Date + Trading time.
    filters = [("ID", "in", list(symbols))] if symbols is not None else None
    t = pd.read_parquet(f"{DATA}/prices-{day}.parquet", filters=filters)
    t = t[t["Trading time"] != PLACEHOLDER].copy()
    t["ts"] = pd.to_datetime(t["Date"], format="%d-%m-%Y") + pd.to_timedelta(t["Trading time"])
    t["day_order"] = 0 if day == MON else 1     # seq restarts every day
    return t


# The verified v3 answer key for the whole week; we only need Monday and Tuesday
key = pd.read_parquet("../answer_key/answer-key-week.parquet")
key_mon = key[key["window_start"].dt.strftime("%d-%m-%y") == MON]
key_tue = key[key["window_start"].dt.strftime("%d-%m-%y") == TUE]
key = pd.concat([key_mon, key_tue], ignore_index=True)

# --- Part 1: the hand-checked symbol, Monday morning ---------------------
first = key_mon[key_mon["ID"] == HAND_CHECKED].sort_values("window_start")
cut_a = first["window_start"].iloc[min(6, len(first)) - 1] + FIVE_MIN
part_a = trades(MON, [HAND_CHECKED])
part_a = part_a[part_a["ts"] < cut_a]
left = args.budget - len(part_a)

# --- Part 2: a symbol with a real crossover on Tuesday --------------------
real = key_tue[(key_tue["signal"] != "") & (~key_tue["first_window"])]
# Earliest real signal per symbol; the slice stops two windows after it
cuts = real.groupby("ID")["window_start"].min() + 3 * FIVE_MIN
if cuts.empty:
    raise SystemExit("No real crossover on Tuesday in the answer key; nothing to slice.")

# Count each candidate's trades from Monday morning up to its cutoff.
# Reading only these symbols and three columns keeps memory low.
counts = {}
for day in (MON, TUE):
    t = pd.read_parquet(f"{DATA}/prices-{day}.parquet", columns=["ID", "Date", "Trading time"],
                        filters=[("ID", "in", list(cuts.index))])
    t = t[t["Trading time"] != PLACEHOLDER]
    ts = pd.to_datetime(t["Date"], format="%d-%m-%Y") + pd.to_timedelta(t["Trading time"])
    inside = ts.values < cuts.reindex(t["ID"]).values
    for sym, n in t[inside]["ID"].value_counts().items():
        counts[sym] = counts.get(sym, 0) + n
counts = pd.Series(counts).sort_values()

if NEAR_TIE in counts and counts[NEAR_TIE] <= left:
    pick = NEAR_TIE
else:
    fits = counts[counts <= left]
    if fits.empty:
        raise SystemExit(f"No Tuesday crossover symbol fits in {left} rows; raise --budget "
                         f"(smallest needs {counts.iloc[0]}).")
    pick = fits.index[0]
    if NEAR_TIE in counts:
        print(f"{NEAR_TIE} needs {counts[NEAR_TIE]} rows, too many; using {pick} instead")

part_b = pd.concat([trades(MON, [pick]), trades(TUE, [pick])])
part_b = part_b[part_b["ts"] < cuts[pick]]

# --- Write the raw-format CSV ---------------------------------------------
# File order is time order, like the real files. Ties keep file order (seq),
# which is how the answer key breaks same-millisecond ties (challenge 7).
rows = (pd.concat([part_a, part_b])
          .sort_values(["ts", "day_order", "seq"], kind="stable"))

# Copy the header block from a real file: licence lines, the column names,
# and the # description line right under them.
with open(args.header_from, encoding="utf-8") as f:
    head = []
    for line in f:
        head.append(line)
        names = [c.strip() for c in line.rstrip("\n").split(",")]
        if "ID" in names and "Last" in names and "Trading time" in names:
            columns = names
            break
    else:
        raise SystemExit("Couldn't find the column header line in --header-from")
    for line in f:                      # description line(s) right under the header
        if not line.startswith("#"):
            break
        head.append(line)
if columns and columns[-1] == "":
    columns = columns[:-1]              # the stray trailing comma (challenge 2)


def cell(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if isinstance(value, float):
        return repr(value)              # shortest text that reads back as the same number
    return str(value)


carried = [c for c in columns if c in rows.columns]   # everything else stays empty
os.makedirs(args.out, exist_ok=True)
with open(f"{args.out}/test-slice.csv", "w", encoding="utf-8") as f:
    f.writelines(head)
    for rec in rows[carried].to_dict("records"):
        f.write(",".join(cell(rec.get(c)) if c in rec else "" for c in columns) + ",\n")

# --- Expected output, straight from the verified answer key ---------------
expected = pd.concat([
    key[(key["ID"] == HAND_CHECKED) & (key["window_start"] < cut_a)],
    key[(key["ID"] == pick) & (key["window_start"] < cuts[pick])],
]).sort_values(["window_start", "ID"])

expected = expected.rename(columns={"ID": "symbol"})
expected["window_start"] = expected["window_start"].dt.strftime("%Y-%m-%dT%H:%M:%S")
expected = expected[["symbol", "SecType", "window_start", "close",
                     "ema38", "ema100", "first_window", "signal"]]

with open(f"{args.out}/test-slice-expected.jsonl", "w") as f:
    for rec in expected.to_dict("records"):
        rec["first_window"] = bool(rec["first_window"])
        f.write(json.dumps(rec) + "\n")         # json keeps every digit
expected.to_csv(f"{args.out}/test-slice-expected.csv", index=False, float_format="%.10f")

# --- A short summary to paste to the team ---------------------------------
print(f"\nWrote {len(rows):,} trade rows and {len(expected)} expected windows to {args.out}/")
for sym, part in ((HAND_CHECKED, part_a), (pick, part_b)):
    e = expected[expected["symbol"] == sym]
    sig = e[(e["signal"] != "") & (~e["first_window"])]
    print(f"  {sym}: {len(part):,} trades, {len(e)} windows, "
          f"{part['ts'].min():%a %H:%M} to {part['ts'].max():%a %H:%M}, "
          f"real signals: {', '.join(sig['signal'] + ' at ' + sig['window_start']) or 'none'}")
