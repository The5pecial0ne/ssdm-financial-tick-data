#!/usr/bin/env python3
"""
Takes the first 10 valid trades from the Monday tick-data CSV, so there's a
tiny file for testing the pipeline.

"Valid" means every column the project actually uses has a real value:
ID, SecType, Date, Time, Last, Trading time and Trading date. (The other
columns are empty in nearly every row, because the dataset authors stripped
most attributes out to keep the files small.) Rows stamped with the
placeholder trade time 01:00:00.000 are skipped too, since that time is fake.

Usage, from the repo root:
    python analysis/sample_csv.py              # first 10 valid rows of Monday
    python analysis/sample_csv.py -n 50        # first 50 instead
"""

import argparse
import csv
import sys

MONDAY_FILE = "data/debs2022-gc-trading-day-08-11-21.csv"
NEEDED = ["ID", "SecType", "Date", "Time", "Last", "Trading time", "Trading date"]
PLACEHOLDER = "01:00:00.000"


def is_valid(row, cols):
    """True if every column we care about is filled and the price is real."""
    try:
        values = {name: row[i].strip() for name, i in cols.items()}
        price = float(values["Last"])
    except (IndexError, ValueError):
        return False
    return all(values.values()) and price > 0 and values["Trading time"] != PLACEHOLDER


def main():
    parser = argparse.ArgumentParser(description="Grab the first valid trades from a DEBS 2022 trading CSV.")
    parser.add_argument("input", nargs="?", default=MONDAY_FILE)
    parser.add_argument("-n", "--rows", type=int, default=10)
    parser.add_argument("-o", "--output", default="analysis/monday_sample.csv")
    args = parser.parse_args()

    cols, kept = None, 0

    # Read line by line, since the day files are huge. We stop as soon as we have enough.
    with open(args.input, newline="", encoding="utf-8") as src, \
         open(args.output, "w", newline="", encoding="utf-8") as dst:
        for line in src:
            if not line.strip():
                continue

            # Keep the licence lines and header on top, so the sample parses like the real file
            if line.startswith("#") or line.startswith("ID,"):
                dst.write(line)
                if line.startswith("ID,"):
                    names = [c.strip() for c in line.rstrip("\r\n").split(",")]
                    cols = {name: names.index(name) for name in NEEDED}
                continue

            if cols is None:
                sys.exit("Couldn't find the column header. Is this a DEBS 2022 trading CSV?")

            if is_valid(next(csv.reader([line])), cols):
                dst.write(line)
                kept += 1
                if kept >= args.rows:
                    break

    print(f"Wrote {kept} valid rows to {args.output}")


if __name__ == "__main__":
    main()