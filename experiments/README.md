# Experiments

Four scripted experiments produce every number and figure the report needs. Each one writes raw
output to `results/<experiment>/<timestamp>/`. Then `python -m experiments.analyze` turns the
newest run of each into figures (`results/figures/`) and tables (`results/summary.md`).

| Script | Answers | Grading criterion | Needs |
|---|---|---|---|
| `run_scalability.sh` | How does processing time change with the number of cores (1, 2, 4 by default; set `CORES`)? How evenly is the load spread (long tail)? Is the Kafka-path output correct for the full week? | Scalability, Performance, Functionality, Evaluation | Docker, data |
| `run_latency.sh` | How long from a window closing to its advisory, at different input rates? | Performance (latency under load) | Docker, data |
| `run_ooo_sweep.sh` | What does the out-of-order bound cost in latency, and what does it buy in correctness? | Creative exploration (trade-off backed by measurements) | Java + Maven, data |
| `run_remaining.sh` | 6 and 8 cores, every event type, each exchange on its own, and latency with no out-of-order slack, then the overview table | Scalability (resources, event volumes, exchanges), Creative exploration | Docker, data |
| `run_local_preview.sh` | Same as scalability, without Docker (Linux only) | | Kafka on localhost |

## Before you start (MacBook)

1. **Docker Desktop**: Settings → Resources. Give it at least **6 CPUs and 8 GB of memory**,
   so a 4-core TaskManager plus Kafka and the JobManager fit. Note the numbers for the report.
2. **Python**: `python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt`
3. **Data**: `python3 scripts/download_data.py` puts the day files in `data/` as
   `data/debs2022-gc-trading-day-08-11-21.csv` and so on (see the main README, "Get the data").
   The scripts stop straight away with a clear message if a file they need is missing.
4. Close other heavy apps. Plug the laptop in. A MacBook Air has no fan and slows down when it gets hot,
   so leave a minute between experiments, and mention this in the limitations.

## 1. Scalability (and full-week correctness)

```bash
./experiments/run_scalability.sh
```

Defaults: all trades of Monday to Friday (about 59 million events), cores `1 2 4`, 3 repeats each.
`CORES="1 2 4 8"` picks other core counts: each one sets the TaskManager's CPU limit, its number of
task slots and the job's parallelism. Nothing has to be typed in while it runs. At the end it prints
time, events/s, speedup and efficiency per core count (saved as `speedup.txt` in the run folder)
and exits with status 1 if a run did not finish or the output differs from the reference.
Filling the topic takes about 20 to 30 minutes, and each run takes a few minutes. Afterwards it dumps the
4-core output and validates it against `answer_key/answer-key-week.parquet`.

Variants worth running for the report:

```bash
# every event type (quotes and housekeeping too) for one busy day: heavier input, same results
DAYS=08-11-21 EVENTS=all ./experiments/run_scalability.sh
# one exchange at a time, to compare exchanges
EXCHANGES=FR  DAYS=08-11-21 ./experiments/run_scalability.sh
EXCHANGES=NL  DAYS=08-11-21 ./experiments/run_scalability.sh
EXCHANGES=ETR DAYS=08-11-21 ./experiments/run_scalability.sh
# a quick run while developing: the busy morning only, one repeat
DAYS=08-11-21 FROM=08:00 TO=12:00 REPEATS=1 ./experiments/run_scalability.sh
```

One script chains the runs that are still missing: the trades week at 1, 2, 4, 6 and 8 cores, the
every-event-type week (about 289 million events), the three per-exchange runs, each validated, and a
latency run with an out-of-order bound of 0. It then regenerates the tables with an overview of all
runs. It takes 1 to 3 hours on a MacBook, so it suits an overnight run:

```bash
./experiments/run_remaining.sh   # SKIP_MORE_CORES=1, SKIP_ALL_EVENTS=1, SKIP_EXCHANGES=1, SKIP_LATENCY=1 leave a part out
```

Runs over the whole week without exchange or time filters are checked against the shipped answer
key, whatever the event types (quotes never reach a window). Filtered runs are checked against a
reference computed for the same filtered input.

Measured: Flink's own job runtime (RUNNING → FINISHED, from the REST API), CPU per container every
second (`docker stats`), and records per parallel instance of every operator (`job-*.json`), which
shows the effect of hot symbols on load balance.

## 2. Latency under load

```bash
./experiments/run_latency.sh
```

Replays Monday 08:00 to 12:00 into a running job at 20k, 50k and 100k events/s, and as fast as possible.
Latency is defined in the script header. It is split into *waiting for the watermark* and *processing*.

Variants:

```bash
CORES="1 2 4" ./experiments/run_latency.sh                         # latency vs cores
JOB_ARGS="--buffer-timeout-ms 5 --watermark-interval-ms 50" ./experiments/run_latency.sh   # tuned for latency
OOO_MS=0 ./experiments/run_latency.sh                              # no out-of-order slack
```

In a cloud test on synthetic data at 100k events/s, the tuned settings cut median latency from about
1.2 s to about 0.13 s. Check whether the real data shows the same.

## 3. Out-of-order bound sweep

```bash
./experiments/run_ooo_sweep.sh            # Monday, bounds 0 s, 1 s, 5 s, 30 s, 120 s
```

Runs without Docker, in file mode. For each bound it reports how many trades arrived too late for
their window, and how many windows and advisories then differ from the reference.

On the real Monday no trade arrives too late even with a bound of 0 (the Mac run in
`results/ooo-sweep/`), so every bound gives the same, correct result there. To see the trade-off, sweep synthetic data with a known amount of
disorder (`CSV=` takes any input). `results/ooo-synthetic/` holds such a run, with the exact commands:

```bash
python -m tools.synth -n 3000000 --out-of-order 0.02 --max-delay-ms 60000 -o data/synthetic-ooo.csv
CSV=data/synthetic-ooo.csv BOUNDS="0 1000 5000 15000 30000 60000 120000" ./experiments/run_ooo_sweep.sh
```

## 4. Figures and tables

```bash
python -m experiments.analyze
open results/summary.md results/figures/
```

By default it takes the newest run of each experiment, and it overwrites `results/summary.md`. To put
several runs side by side, name them. This is the exact command behind the committed
`results/summary.md`, and on a fresh clone it rebuilds that file unchanged:

```bash
python -m experiments.analyze \
  --scalability results/scalability/20261125-223400-cores-1-8-r3 results/scalability/20261124-132305-cores-1-8 \
                results/scalability/20261117-221652 results/scalability/20261124-132305-all-events \
                results/scalability/20261124-132305-FR results/scalability/20261124-132305-NL \
                results/scalability/20261124-132305-ETR \
  --latency results/latency/20261125-223400 results/latency/20261117-224744 results/latency/tuned \
            results/latency/20261124-132305-ooo0 \
  --ooo results/ooo-sweep/20261125-223400
```

The latency runs' per-window dumps (`results-*.jsonl`, hundreds of MB) are not in git. Without them
the script reads each run's `summary.csv` instead and keeps the latency distribution figures that are
already in `results/figures/`.
