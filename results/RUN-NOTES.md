# Mac run, 17 Nov 2026

Raw record of how the results in this folder were produced. Not report text.

## Setup

- Hardware: Apple M5, 10 cores (`hw.ncpu`), 16 GB RAM (`hw.memsize` 17179869184), MacBook Air, on mains power.
- Docker Desktop 29.8.0: 10 CPUs, 7.75 GiB memory (the 8 GB setting).
- Java: Homebrew openjdk@17 (17.0.21) for the local file-mode runs; Maven 3.10.1. Python 3.14.7 venv.
- Data: DEBS 2022 day CSVs 08 to 12 Nov 2021 from Zenodo record 6382482, md5 verified.

## Wall-clock time per step

| Step | Time |
|---|---|
| Quick check (`run-local.sh` on `analysis/test-slice.csv` + validate) | 23 s, PASSED |
| `run_scalability.sh` (Kafka fill 182 s at 326k ev/s, 9 runs, dump) | 729 s |
| Full-week validation (re-run by hand after fix 1) | 8 s, PASSED |
| `run_latency.sh` (default) | 374 s |
| `run_latency.sh` (tuned: `--buffer-timeout-ms 5 --watermark-interval-ms 50`) | 373 s |
| Re-run of the 20k ev/s rate, default + tuned, after fix 3 | 434 s |
| `run_ooo_sweep.sh` | 161 s |
| `python -m experiments.analyze --latency results/latency/20261117-224744 results/latency/tuned` | 2 s |

## Changes to the scripts

1. `experiments/run_scalability.sh`: line 66 used `$EXCHANGES` without a default under `set -u`, so the
   validation step crashed with "unbound variable" after all runs finished. Now `${EXCHANGES:-}`.
2. `experiments/analyze.py`: `--latency` takes several runs (one table each, figures suffixed with the
   run folder name). Without it, "newest run" sorted `tuned` after the timestamped default run and
   silently dropped the default run from `summary.md`.
3. `experiments/common.sh` and `docker-compose.yml`: topics are created with `retention.ms=-1`.
   Flink writes output records with their event-time timestamp (Nov 2021), so Kafka's default 7-day
   retention deleted `ema-results` segments at its 5-minute retention check during a run. This hit both
   20k ev/s latency runs (5,891 and 83,149 of 96,765 windows dumped; Kafka log: "Deleting segment ... due to
   log retention time 604800000ms breach"). Those two runs were re-run with the fix and their files
   replaced (`settings-rerun-rate20000.json`, `../rerun-rate20000.log`). All other runs had complete output
   (96,765 windows per latency run; the scalability output passed full-week validation).

## Not committed

Raw dumps over 25 MB (`flink-output.jsonl` 582 MB, `flink-ooo*.jsonl` 99 MB each, `results-*.jsonl`
~30 MB each, `reference.parquet`) are gitignored. They stay in the local clone on the Mac.

## Notes

- The harmless `BrokenPipeError` in the latency logs is the CPU sampler being killed in the middle of a write.
- A first scalability attempt was killed during the Kafka fill when the controlling session ended; it was
  deleted and restarted from scratch.

# Mac rerun, 25 Nov 2026

Raw record of the repeat pass. Earlier runs were kept; the new ones sit next to them.

- Hardware: same MacBook Air M5. Core split from `sysctl`: `hw.perflevel0.physicalcpu` 4 (performance),
  `hw.perflevel1.physicalcpu` 6 (efficiency). Docker Desktop 29.8.0, 10 CPUs, 7.75 GiB. On mains power
  (it was plugged in at 34% battery just before the run, so it was charging throughout).
- 22:28 clean start (`docker compose --profile app down --volumes`), then `--profile app up -d --build job producer`
  (the dashboard container was left out because a local Streamlit dashboard already held port 8501).
  Producer sent 59,391,670 events in 270.7 s. TESTING.md section 3 validation: PASSED, 15,272 windows, 174 signals.
- 22:34 to 23:07, one chain (log in `results/logs/chain-20261125-223400.log`), under `caffeinate -i`:
  - `OUT=results/scalability/20261125-223400-cores-1-8-r3 CORES="1 2 3 4 5 6 7 8" REPEATS=3 ./experiments/run_scalability.sh`
    (full-week validation of the 8-core output: PASSED, 1,855,645 windows, 4,970 symbols, 11,349 signals)
  - `OUT=results/latency/20261125-223400 ./experiments/run_latency.sh`
  - `OUT=results/ooo-sweep/20261125-223400 ./experiments/run_ooo_sweep.sh` (Java 17 from Homebrew; 0 late trades at every bound, all passed)
- Analysis: `python -m experiments.analyze` with all scalability and latency runs named, `--ooo results/ooo-sweep/20261125-223400`.
