# Experiments

Four scripted experiments produce every number and figure the report needs. Each one writes raw
output to `results/<experiment>/<timestamp>/`. Then `python -m experiments.analyze` turns the
newest run of each into figures (`results/figures/`) and tables (`results/summary.md`).

| Script | Answers | Grading criterion | Needs |
|---|---|---|---|
| `run_scalability.sh` | How does processing time change with 1, 2, 4 cores? How evenly is the load spread (long tail)? Is the Kafka-path output correct for the full week? | Scalability, Performance, Functionality, Evaluation | Docker, data |
| `run_latency.sh` | How long from a window closing to its advisory, at different input rates? | Performance (latency under load) | Docker, data |
| `run_ooo_sweep.sh` | What does the out-of-order bound cost in latency, and what does it buy in correctness? | Creative exploration (trade-off backed by measurements) | Java + Maven, data |
| `run_remaining.sh` | Every event type, and each exchange on its own, at 1, 2 and 4 cores, then the overview table | Scalability (event volumes, exchanges) | Docker, data |
| `run_local_preview.sh` | Same as scalability, without Docker (Linux only) | | Kafka on localhost |

## Before you start (MacBook)

1. **Docker Desktop**: Settings → Resources. Give it at least **6 CPUs and 8 GB of memory**,
   so a 4-core TaskManager plus Kafka and the JobManager fit. Note the numbers for the report.
2. **Python**: `python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt`
3. **Data**: the day files must be in `data/` as `data/debs2022-gc-trading-day-08-11-21.csv` and so on
   (see the main README, "Get the data").
4. Close other heavy apps. Plug the laptop in. A MacBook Air has no fan and slows down when it gets hot,
   so leave a minute between experiments, and mention this in the limitations.

## 1. Scalability (and full-week correctness)

```bash
./experiments/run_scalability.sh
```

Defaults: all trades of Monday to Friday (about 59 million events), cores `1 2 4`, 3 repeats each.
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

For the whole week, one script chains the every-event-type run (about 289 million events) and the
three per-exchange runs, each validated, and then regenerates the tables with an overview of all
runs. It takes about 3 hours on a MacBook, so it suits an overnight run:

```bash
./experiments/run_remaining.sh          # SKIP_ALL_EVENTS=1 or SKIP_EXCHANGES=1 leaves a part out
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

By default it takes the newest run of each experiment. To put several runs side by side, name them:

```bash
python -m experiments.analyze --scalability results/scalability/*/ --latency results/latency/20261117-224744 results/latency/tuned
```

## What's already in `results/preview-cloud/`

A scalability preview run in a 4-core Linux cloud container on **synthetic** data
(`tools/synth.py`, 12M events), using `run_local_preview.sh`. It shows the harness works and roughly
what to expect (about 300k events/s on 1 core and 830k on 4). It is not the real dataset, so don't put
it in the report as your result. Your own Mac runs replace it.
