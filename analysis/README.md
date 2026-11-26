# Analysis: data exploration and the answer key

These are the pandas scripts we used to explore the DEBS 2022 data and to build the answer key that
the Flink output is checked against. Except for `sample_csv.py`, they run from inside this folder and read and write `../data/`,
so the day files have to be downloaded first (main README, "Get the data").

| Script | What it does | Reads | Writes |
|---|---|---|---|
| `load_day.py <day>` | Reads one day file in chunks and keeps the trades (`Last > 0`), plus event counts per symbol | `data/debs2022-gc-trading-day-<day>.csv` | `data/prices-<day>.parquet`, `data/counts-<day>.parquet` |
| `answer_key_v3.py <day> ...` | The answer key: 5-minute closes, EMA38 and EMA100 carried across days, buy and sell crossovers. Give the days in date order | `data/prices-*.parquet` | `data/answer-key-v3-<day>.parquet` |
| `finalise.py` | Joins the five weekday keys into one file | `data/answer-key-v3-*.parquet` | `data/answer-key-week.parquet`, `data/signals-week.csv` |
| `make_test_slice.py` | Cuts the small real excerpt used by the quick check and the Docker demo (two Frankfurt symbols, Monday and Tuesday), with the answer-key rows it must reproduce | `data/prices-0{8,9}-11-21.parquet`, the week key | `test-slice.csv`, `test-slice-expected.jsonl` (written to `data/test-slice/`, then copied here) |
| `longtail.py` | Events and trades per symbol over the week (the long-tail figure) | `data/counts-*.parquet`, `data/prices-*.parquet` | a PNG |
| `clocks.py`, `late_check.py` | How the two clocks (`Time`, `Trading time`) relate, and whether trades arrive out of order | `data/prices-*.parquet` | printed only |
| `warmup.py` | How long EMA100 takes to warm up from 0 | `data/answer-key-v3-08-11-21.parquet` | printed only |
| `sample_csv.py` | The first 10 valid Monday trades, a tiny file for trying the pipeline. Runs from the repository root, unlike the others | `data/debs2022-gc-trading-day-08-11-21.csv` | `analysis/monday_sample.csv` |

To rebuild the shipped answer key from the raw data:

```bash
cd analysis
for d in 08 09 10 11 12; do python load_day.py $d-11-21; done
python answer_key_v3.py 08-11-21 09-11-21 10-11-21 11-11-21 12-11-21
python finalise.py
```

`data/answer-key-week.parquet` should then equal `answer_key/answer-key-week.parquet` (1,855,645 windows,
4,970 symbols, 11,349 advisories). The day-by-day keys in `answer_key/` are the same rows split by day.
Both scripts also compare against an earlier version of the key (v2, before placeholder times were
dropped). Those files only exist in our own `data/` folder, so the comparison is skipped without them.

`answer_key.csv` is a 20-row excerpt of the key (symbol 120071.ETR, Monday morning) that is easy to read by eye.

You don't need any of this to check the Flink job: `tools/reference.py` computes the same reference
from any input file, and `tools/validate.py` compares against it.
