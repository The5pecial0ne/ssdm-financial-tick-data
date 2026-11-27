"""
Downloads the DEBS 2022 trading data from Zenodo (record 6382482, CC BY-NC-SA 4.0) into data/,
under the names the producer and the experiment scripts expect.

    python3 scripts/download_data.py                  # Monday to Friday, about 25 GB
    python3 scripts/download_data.py --days 08-11-21  # only Monday, about 5 GB
    python3 scripts/download_data.py --list           # show what the record holds, download nothing

It asks Zenodo's API which files the record has, so it doesn't depend on hard-coded links. Files
that are already complete are skipped, an interrupted download carries on where it stopped, and
every file is checked against the checksum Zenodo publishes. Zipped or gzipped files are unpacked.
Only the standard library is used, so it runs before `pip install`.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import shutil
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
RECORD = "6382482"
API = f"https://zenodo.org/api/records/{RECORD}"
WEEKDAYS = ["08-11-21", "09-11-21", "10-11-21", "11-11-21", "12-11-21"]
WEEK = WEEKDAYS + ["13-11-21", "14-11-21"]


def fail(msg: str):
    sys.exit(f"download failed: {msg}")


def get_json(url: str) -> dict:
    for attempt in range(5):
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                return json.load(r)
        except Exception as e:  # Zenodo sometimes answers 429 or times out; try again a few times
            if attempt == 4:
                fail(f"could not read {url}: {e}")
            time.sleep(5 * (attempt + 1))


def md5_of(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 22), b""):
            h.update(block)
    return h.hexdigest()


def fetch(url: str, dest: Path, size: int):
    """Downloads url to dest, resuming a partial file, with a progress line."""
    for attempt in range(5):
        have = dest.stat().st_size if dest.exists() else 0
        if have >= size:
            return
        req = urllib.request.Request(url, headers={"Range": f"bytes={have}-"} if have else {})
        try:
            with urllib.request.urlopen(req, timeout=120) as r, open(dest, "ab" if have else "wb") as out:
                if have and r.status != 206:  # the server ignored the range request: start over
                    out.truncate(0)
                    have = 0
                done, last = have, 0.0
                while block := r.read(1 << 20):
                    out.write(block)
                    done += len(block)
                    if time.time() - last > 2:
                        print(f"\r  {done / 1e9:6.2f} / {size / 1e9:.2f} GB", end="", flush=True)
                        last = time.time()
            print(f"\r  {done / 1e9:6.2f} / {size / 1e9:.2f} GB")
        except Exception as e:
            print(f"\n  interrupted ({e}), retrying")
            time.sleep(5 * (attempt + 1))
    if not dest.exists() or dest.stat().st_size < size:
        fail(f"{dest.name} is incomplete after 5 attempts; run the script again to carry on")


def unpack(archive: Path, target: Path):
    print(f"  unpacking {archive.name}")
    tmp = target.with_suffix(".csv.part")
    if archive.suffix == ".gz":
        with gzip.open(archive, "rb") as src, open(tmp, "wb") as out:
            shutil.copyfileobj(src, out, 1 << 22)
    else:
        with zipfile.ZipFile(archive) as z:
            csvs = [n for n in z.namelist() if n.endswith(".csv")]
            if len(csvs) != 1:
                fail(f"{archive.name} holds {len(csvs)} CSV files, expected 1")
            with z.open(csvs[0]) as src, open(tmp, "wb") as out:
                shutil.copyfileobj(src, out, 1 << 22)
    tmp.replace(target)
    archive.unlink()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", default="weekdays", help="weekdays (default), week, or e.g. 08-11-21,09-11-21")
    ap.add_argument("--data-dir", default=str(REPO / "data"))
    ap.add_argument("--list", action="store_true", help="only list the record's files")
    a = ap.parse_args()

    days = WEEK if a.days == "week" else WEEKDAYS if a.days == "weekdays" else \
        [d.strip() for d in a.days.split(",") if d.strip()]
    files = get_json(API)["files"]
    if a.list:
        for f in files:
            print(f"{f['key']:55} {f['size'] / 1e9:6.2f} GB")
        return

    data = Path(a.data_dir)
    data.mkdir(parents=True, exist_ok=True)
    for day in days:
        target = data / f"debs2022-gc-trading-day-{day}.csv"
        if target.exists() and target.stat().st_size > 0:
            print(f"{target.name}: already there, skipped")
            continue
        matches = [f for f in files if day in f["key"]]
        if len(matches) != 1:
            fail(f"expected one file for {day} in Zenodo record {RECORD}, found "
                 f"{[f['key'] for f in matches]}; see --list")
        f = matches[0]
        print(f"{f['key']} ({f['size'] / 1e9:.2f} GB) -> {target}")
        name = f["key"].rsplit("/", 1)[-1]
        part = data / (name + ".download")
        fetch(f["links"]["self"], part, f["size"])

        algo, _, expected = f.get("checksum", "").partition(":")
        if algo == "md5":
            print("  checking the checksum")
            if md5_of(part) != expected:
                part.unlink()
                fail(f"{name} does not match Zenodo's checksum and was deleted; run the script again")
        if name.endswith((".zip", ".gz")):
            archive = part.rename(data / name)
            unpack(archive, target)
        else:
            part.rename(target)
        print(f"  saved {target.name}")
    print(f"done: {len(days)} day file(s) in {data}")


if __name__ == "__main__":
    main()
