# Out-of-order sweep on synthetic data

On the real Monday no trade arrives too late, even with an out-of-order bound of 0 (the Mac run in
`results/ooo-sweep/`), so that day can't show what the bound costs or buys. This run uses
generated data with a known amount of disorder instead.

**Input:** `tools/synth.py`, 3,000,000 events in the DEBS 2022 layout, 2% of trades delivered
up to 60 s after their trading time (plus the usual quotes, placeholder times and long tail):

```bash
python -m tools.synth -n 3000000 --out-of-order 0.02 --max-delay-ms 60000 -o data/synthetic-ooo.csv
CSV=data/synthetic-ooo.csv OUT=results/ooo-synthetic/ooo-sweep/run \
  BOUNDS="0 1000 5000 15000 30000 60000 120000" ./experiments/run_ooo_sweep.sh
python -m experiments.analyze --results-dir results/ooo-synthetic
```

**Where it ran:** a 4-core Linux cloud container (x86_64), file mode, parallelism 4
(`ooo-sweep/run/settings.json`).

**What's here**

| File | Contents |
|---|---|
| `summary.md` | the table and both figures |
| `ooo-sweep/run/sweep.csv` | per bound: run time, late trades, passed |
| `ooo-sweep/run/summary.csv` | the same plus windows and advisories that differ from the reference |
| `ooo-sweep/run/validation-ooo<bound>.json` | the full validator report for each bound |
| `ooo-sweep/run/flink-ooo<bound>.late.csv` | the trades Flink set aside as late |
| `figures/ooo-late.png`, `figures/ooo-windows.png` | late trades and wrong windows against the bound |

The Flink output (`flink-ooo*.jsonl`) and the reference (`reference.parquet`) are not committed
because they are large. The commands above recreate them in about two minutes.

## The counts vary between runs

Running the sweep again does not give the same numbers. Flink emits watermarks on a wall-clock timer
(every 200 ms), so whether a delayed trade arrives after the watermark has passed its window depends
on timing. On 26 Nov 2026 we regenerated the input with the command above and ran the sweep eight times
on the same kind of machine (4-core x86_64 cloud container, parallelism 4; repeats 2 and 3 only tried
some bounds). `repeats/repeats.csv` has every run:

| Bound | Runs | Wrong windows (min to max) | Runs with any wrong window |
|---:|---:|---:|---:|
| 0 s | 8 | 0 to 347 | 6 |
| 5 s | 8 | 0 to 114 | 5 |
| 15 s | 3 | 57 to 302 | 3 |
| 30 s | 6 | 0 to 114 | 1 |
| 60 s | 6 | 0 | 0 |
| 120 s | 6 | 0 | 0 |

So the trend in the table above holds (small bounds usually give wrong windows, 60 s and more never
did), but a single run's counts should not be read as exact. The input was regenerated, and `tools/synth.py`
output can differ slightly between library versions, so the input is not byte-identical to the one in `ooo-sweep/run/`.
