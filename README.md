# Detecting trading trends in financial tick data

Course project for CS-E4780 Scalable Systems and Data Management (Aalto, 2026).

The system replays the [DEBS 2022 Grand Challenge tick data](https://doi.org/10.5281/zenodo.6382482)
through Kafka into a Flink job that, for every symbol:

- **Query 1:** takes the last trade price of each 5-minute tumbling window and updates two
  exponential moving averages, EMA38 and EMA100.
- **Query 2:** raises a **buy** advisory when EMA38 crosses above EMA100 and a **sell** advisory
  when it crosses below.

A dashboard shows the averages and the advisories live.

```
CSV day files ──> producer ──> Kafka: trading-events (8 partitions, keyed by symbol)
                                   │
                                   ▼
                    Flink: parse ─> trades only ─> key by symbol ─> 5-min event-time window
                           ─> last trade ─> EMA38 / EMA100 ─> crossover?
                                   │
        ┌──────────────────────────┼─────────────────────────┐
        ▼                          ▼                         ▼
 Kafka: ema-results        Kafka: advisories         Kafka: late-trades
 (Query 1, subscribed      (Query 2, buy / sell)     (trades that arrived after
  symbols only)                                       their window had closed)
        │
        ▼
   dashboard (Streamlit, http://localhost:8501)
```

## What you need

- Docker Desktop (or Docker Engine with the compose plugin). Give it at least 4 CPU cores and 6 GB of memory.
- Python 3.9 or newer, only to start the experiment scripts. They use nothing outside the standard library.

Java, Maven, Kafka and Flink are **not** needed on your machine: everything is built and run inside containers.
This works the same on macOS, Linux and Windows.

## 1. Live demo

```
docker compose up --build
```

- Dashboard: http://localhost:8501
- Flink UI: http://localhost:8081

This replays a small real slice that ships with the repo (`analysis/test-slice.csv`: 1,917 trades of two
indices on 8 and 9 November 2021). Stop with `Ctrl+C`, clean up with `docker compose down -v`.

To replay real data instead, put the day files from Zenodo in `data/` and set `REPLAY_ARGS`, for example

```
REPLAY_ARGS="data/debs2022-gc-trading-day-08-11-21.csv --from-time 09:00 --to-time 11:00 --rate 20000" docker compose up --build
```

(on Windows PowerShell: `$env:REPLAY_ARGS="..."; docker compose up --build`).

Which symbols are published is set in `config/subscriptions.txt`. The job always computes every symbol.

## 2. Correctness check

The expected output comes from an independent pandas implementation (`analysis/answer_key_v3.py`),
stored in `answer_key/answer-key-week.parquet`.

```
python experiments/run_correctness.py                    # built-in slice, about a minute, no dataset needed
python experiments/run_correctness.py --days 08-11-21    # all of Monday, needs the day file in data/
```

It loads the trades into Kafka, runs the Flink job over them, collects the output and compares every
window (close, EMA38, EMA100, signal) with the answer key. `RESULT: PASS` and exit code 0 mean everything matched.

## 3. Scalability benchmark

```
python experiments/run_benchmark.py --files data/debs2022-gc-trading-day-08-11-21.csv --from-time 09:00 --to-time 10:00 --workers 1,2,4,8 --repeats 3
```

The slice is loaded into Kafka once. Then, for each worker count, a fresh Flink TaskManager limited to that many
CPU cores processes the whole topic as fast as it can. Results land in `experiments/results/<name>/`:

| File | What it holds |
| --- | --- |
| `summary.csv` | one row per run: seconds, events per second, latency percentiles, CPU and memory per container |
| `scaling.csv` | one row per worker count: median over the repeats, speedup, efficiency |
| `throughput.png`, `speedup.png`, `latency.png`, `cpu.png` | the plots |
| `runs.json`, `raw/` | machine description, settings and every raw output line |

Latency is measured from the moment Flink computes a result (`detected_at`) to the moment the Kafka broker
stores it (the broker's own timestamp on the output topic).

Every run also gets a fingerprint of its output, so the summary shows whether 1, 2, 4 and 8 workers
produced identical results.

## 4. The Docker images

`docker compose build` produces two images:

| Image | Contents |
| --- | --- |
| `ssdm-flink:latest` | Flink 1.20 with the job jar (built by Maven inside the image build). Runs as JobManager, TaskManager and job submitter. |
| `ssdm-tools:latest` | Python: producer, dashboard, result collection, comparison, plots, the test slice. |

To hand them over as files:

```
docker compose build
docker save ssdm-flink:latest ssdm-tools:latest -o ssdm-images.tar
# on the other machine
docker load -i ssdm-images.tar
docker compose up
```

Images built on an Apple-silicon Mac are `arm64`. For a machine with an Intel or AMD processor, build there,
or build with `docker buildx build --platform linux/amd64`.

## Repository layout

| Path | What |
| --- | --- |
| `flink-job/` | The Flink job (Java 11) and its Dockerfile |
| `producer/producer.py` | Replays CSV day files into Kafka |
| `dashboard/dashboard.py` | Live dashboard |
| `tools/` | `collect.py` (Kafka topic to file), `compare.py` (output against answer key), `summarize.py` (benchmark tables and plots) |
| `experiments/` | `run_correctness.py`, `run_benchmark.py` |
| `config/subscriptions.txt` | Symbols whose results are published |
| `analysis/`, `answer_key/` | Data exploration and the pandas answer key |
| `DECISION.MD` | Design decisions, in date order |

## How the job treats the data

- A row is a trade when `Last` is a number above 0 and `Trading time` is present and is not `01:00:00.000`
  (a placeholder the feed uses when the real time is missing). Everything else is filtered out inside Flink.
- Event time is `Date` + `Trading time`. The `Trading date` column is empty in the files.
- Windows are 5 minutes long, do not overlap and are aligned to midnight exchange time (CET).
- A window closes once the watermark passes its end. The watermark trails the newest trade by
  5 seconds (`--max-out-of-order-ms`). Trades that arrive later than that go to the `late-trades` topic.
- Both averages start at 0, as the project description defines. A symbol's first window therefore always
  looks like a crossover; it is flagged `first_window` and is not published as an advisory.
- A window without trades produces no output and leaves the averages unchanged.
