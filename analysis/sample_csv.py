#!/usr/bin/env python3
"""
Cuts the first few rows out of the Monday tick-data CSV, so there's a tiny
file for testing the pipeline instead of pushing the whole day through it.

Usage:
    python head_ticks.py                        # first 10 rows of Monday
    python head_ticks.py -n 50                  # first 50 rows instead
    python head_ticks.py --priced-only          # only rows that have a Last price
    python head_ticks.py some/other/day.csv -o sample.csv
"""

import argparse
import csv
import sys

MONDAY_FILE = "data/debs2022-gc-trading-day-08-11-21.csv"


def header_columns(line):
    """If this line is the column header, return its column names. Otherwise None.

    The header could be a plain row or sit inside a '# ...' comment,
    so we strip any leading '#' before checking.
    """
    cleaned = line.lstrip("# ").rstrip("\r\n")
    if cleaned.startswith("ID,"):
        return [c.strip() for c in cleaned.split(",")]
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Cut a small test sample out of a DEBS 2022 trading CSV."
    )
    parser.add_argument("input", nargs="?", default=MONDAY_FILE)
    parser.add_argument("-n", "--rows", type=int, default=10)
    parser.add_argument("-o", "--output", default="monday_sample.csv")
    parser.add_argument(
        "--priced-only",
        action="store_true",
        help="skip rows with no Last trade price (lots of rows are just bid/ask updates)",
    )
    args = parser.parse_args()

    last_col = None
    kept = 0

    # Go line by line. The day files are huge, so we never read them in whole.
    with open(args.input, newline="", encoding="utf-8") as src, open(
        args.output, "w", newline="", encoding="utf-8"
    ) as dst:
        for line in src:
            if not line.strip():
                continue

            # Copy comments and the header through as they are, so the sample
            # looks exactly like the real file to whatever parses it.
            columns = header_columns(line)
            if columns is not None or line.startswith("#"):
                dst.write(line)
                if columns and "Last" in columns:
                    last_col = columns.index("Last")
                continue

            if args.priced_only:
                if last_col is None:
                    sys.exit("Couldn't find a 'Last' column in the header, "
                             "so --priced-only can't work on this file.")
                row = next(csv.reader([line]))
                if last_col >= len(row) or not row[last_col].strip():
                    continue

            dst.write(line)
            kept += 1
            if kept >= args.rows:
                break

    print(f"Wrote {kept} rows from {args.input} to {args.output}")


if __name__ == "__main__":
    main()