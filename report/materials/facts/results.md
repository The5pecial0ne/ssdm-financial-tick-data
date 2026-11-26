# Experiment results, all in one place

Every number here is from `results/` (tables in `results/summary.md`, extra numbers in
`derived-facts.md` next to this file). Observations are stated as measurements; the "why" is left
for you. Where something is a single run, it says so.

Runs:
- **17 Nov** (`20261117-221652`): weekday trades, 1 / 2 / 4 cores, 3 repeats each; latency default
  and tuned; real-Monday OOO sweep.
- **24 Nov** (`20261124-132305-*`): weekday trades at 1 / 2 / 4 / 6 / 8 cores; all event types, FR,
  NL, ETR at 1 / 2 / 4 cores; latency with bound 0. **One repeat per point.**
- **Synthetic OOO sweep** (`results/ooo-synthetic/`): cloud container, 3 M synthetic events.

## 1. Correctness (every Kafka run passed)

| Check (weekday trades, all exchanges) | Reference | Flink |
|---|---|---|
| Windows | 1,855,645 | 1,855,645 matched, 0 missing, 0 extra |
| Symbols | 4,970 | 4,970 |
| Windows with a value mismatch | | 0 |
| Max relative error close / EMA38 / EMA100 | | 2.2e-16 / 5.6e-12 / 8.6e-12 |
| Advisories, all | 11,349 | 11,349 matched; 0 missed, 0 spurious, 0 wrong direction |
| Advisories, not in a first window | 6,379 | 6,379 |
| First-window flag mismatches | | 0 |

Per exchange (24 Nov): FR 1,043,637 windows / 6,049 advisories, NL 255,249 / 1,409, ETR 556,759 /
3,891, all passed. All-events run: same output as trades-only, passed. Docker demo: 330 / 330
windows, 3 / 3 advisories (also in CI).

## 2. Throughput and scalability

Weekday trades, 59,391,670 events, 24 Nov, 1 run each:

| Cores | Time (s) | M events/s | Speedup | Efficiency | TM CPU (cores) | CPU per core |
|---|---|---|---|---|---|---|
| 1 | 68.6 | 0.87 | 1.00 | 1.00 | 0.93 | 93% |
| 2 | 42.6 | 1.39 | 1.61 | 0.80 | 1.89 | 94% |
| 4 | 24.1 | 2.47 | 2.85 | 0.71 | 3.55 | 89% |
| 6 | 20.3 | 2.92 | 3.37 | 0.56 | 4.71 | 79% |
| 8 | 14.5 | 4.08 | 4.71 | 0.59 | 6.04 | 76% |

Step gains: 1→2 cores −38% time, 2→4 −43%, 4→6 −16%, 6→8 −29%.

17 Nov, same input, 3 repeats (mean ± std): 1 core 68.6 ± 3.1 s, 2 cores 42.5 ± 0.6 s, 4 cores
25.5 ± 0.7 s (speedup 1.61 and 2.69). The 24 Nov 4-core point (24.1 s) is 1.4 s faster than the 17 Nov mean.

Other inputs, 24 Nov:

| Input | Events | 1 core | 2 cores | 4 cores | Speedup 2 / 4 | M events/s at 4 | CPU/core at 4 |
|---|---|---|---|---|---|---|---|
| All event types | 289.1 M | 147.5 s | 86.7 s | 55.4 s | 1.70 / 2.66 | 5.22 | 92% |
| FR trades | 21.4 M | 31.8 s | 19.6 s | 11.3 s | 1.63 / 2.81 | 1.89 | 65% |
| ETR trades | 32.0 M | 39.0 s | 22.2 s | 13.8 s | 1.76 / 2.82 | 2.31 | 70% |
| NL trades | 6.0 M | 13.2 s | 9.1 s | 5.0 s | 1.44 / 2.64 | 1.21 | 42% |

## 3. Where the time goes inside Flink (new, from the REST metrics)

Share of each task's time spent busy / backpressured / idle, weekday trades, 24 Nov:

| Cores | Source + router | Window | EMA + sinks |
|---|---|---|---|
| 1 | 57 / 15 / 28 | 64 / 5 / 31 | 17 / 0 / 83 |
| 2 | 60 / 13 / 28 | 68 / 3 / 29 | 19 / 0 / 81 |
| 4 | 69 / 7 / 24 | 70 / 2 / 29 | 20 / 0 / 80 |
| 6 | 81 / 2 / 16 | 58 / 1 / 40 | 17 / 0 / 83 |
| 8 | 84 / 2 / 14 | 69 / 1 / 31 | 25 / 0 / 75 |

All-events run at 4 cores: source + router 72 / 1 / 27, window 38 / 0 / 62.

## 4. Load per parallel instance (long tail)

Figure: `../figures/subtask-load.png`.

| Cores | Source (Kafka partitions) max/mean | Window (after keyBy) max/mean | EMA max/mean |
|---|---|---|---|
| 4 | 1.18 | 1.10 | 1.03 |
| 6 | 1.68 | 1.16 | 1.07 |
| 8 | 1.26 | 1.15 | 1.08 |

- The 8 Kafka partitions hold 9.34, 9.01, 5.91, 7.60, 8.21, 8.31, 5.81, 5.21 M trades.
- At 6 cores, source subtasks 0 and 1 read two partitions each (15.24 M and 16.61 M); the other
  four read one (5.2-8.3 M).
- At 4 cores each source subtask reads exactly two partitions (11.7-17.5 M).
- Window operator at 4 cores, other inputs: FR 1.05, NL 1.07, ETR 1.14, all events 1.10.

## 5. Resource use per container

Mean CPU in cores during the measured run (docker stats, about one sample every 2 s; short runs
have few samples). Figure: `../figures/container-cpu.png`.

| TM cores | TaskManager | Kafka | JobManager | TM memory peak |
|---|---|---|---|---|
| 1 | 0.93 | 0.19 | 0.02 | 1.5 GiB |
| 2 | 1.89 | 0.26 | 0.12 | 1.8 GiB |
| 4 | 3.55 | 0.18 | 0.21 | 1.9 GiB |
| 6 | 4.71 | 0.23 | 0.23 | 2.0 GiB |
| 8 | 6.04 | 0.44 | 0.32 | 2.0 GiB |

Kafka memory peak 1.4 GiB (3.6 GiB in the all-events run at 4 cores). Producer: idle during
measured runs. GPU: none used.

## 6. Latency (4 cores, Mon 08:00-12:00, 3 M trades, 96,765 windows per run)

Definition (diagram 04): latency = emitTs − B, where B is when the producer sent the first trade
of the next window. wait = firedTs − B, processing = emitTs − firedTs. All in ms.

| Config | Rate (ev/s) | Achieved | p50 | p95 | p99 | max | wait p50 | wait p99 | proc p50 | proc p99 | advisory p99 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Default (17 Nov) | 20k | 19,997 | 408 | 524 | 621 | 623 | 385 | 522 | 3 | 102 | 510 |
| | 50k | 49,980 | 276 | 398 | 464 | 467 | 261 | 364 | 3 | 105 | 394 |
| | 100k | 99,940 | 266 | 400 | 557 | 570 | 239 | 463 | 2 | 115 | 521 |
| | max | 291,546 | 206 | 490 | 516 | 522 | 198 | 462 | 4 | 203 | 448 |
| Tuned: buffer 5 ms, watermark 50 ms (17 Nov) | 20k | 19,997 | 256 | 285 | 464 | 468 | 253 | 463 | 3 | 46 | 285 |
| | 50k | 49,984 | 140 | 170 | 370 | 452 | 137 | 332 | 3 | 34 | 450 |
| | 100k | 99,939 | 93 | 191 | 381 | 396 | 89 | 301 | 2 | 115 | 374 |
| | max | 289,400 | 80 | 521 | 546 | 555 | 77 | 489 | 2 | 229 | 447 |
| Bound 0, default knobs (24 Nov) | 20k | 19,998 | 237 | 352 | 422 | 454 | 224 | 349 | 3 | 105 | 362 |
| | 50k | 49,993 | 230 | 327 | 364 | 402 | 223 | 310 | 3 | 102 | 359 |
| | 100k | 99,985 | 232 | 308 | 369 | 386 | 222 | 304 | 2 | 100 | 305 |
| | max | 313,135 | 252 | 466 | 478 | 505 | 239 | 437 | 4 | 133 | 360 |

Observations (measurements, not explanations):
- Processing p50 is 2-4 ms in every row; wait p50 is 90-99% of latency p50.
- Default and tuned: p50 falls as the rate rises. Bound 0: p50 stays at 230-252 ms.
- Tuned p95/p99 at max rate are higher than at 100k (521 vs 191, 546 vs 381).
- Bound-0 and default runs were on different days (24 vs 17 Nov), so they differ in more than the bound.
- The 20k runs were re-run on 17 Nov after the retention fix (`RUN-NOTES.md`).

Measurement caveats: emitTs is taken inside Flink before the Kafka sink; B is the host clock and
firedTs / emitTs the Docker VM's clock.

## 7. Out-of-order bound

Real Monday (Mac, file mode, parallelism 4, 17 Nov): 0 late trades and PASSED at 0, 1, 5, 30 and
120 s; run time 24.0-25.2 s; 4,840 expected advisories all correct.

Synthetic, 3 M events, 2% of trades delayed up to 60 s (cloud container):

| Bound (s) | Run time (s) | Late trades | Windows wrong | Advisories missed or wrong (of expected) | Passed |
|---|---|---|---|---|---|
| 0 | 12.0 | 14 | 517 | 4 (4,966) | no |
| 1 | 10.8 | 2 | 229 | 2 (4,968) | no |
| 5 | 11.5 | 5 | 114 | 1 (4,969) | no |
| 15 | 10.9 | 2 | 114 | 1 (4,969) | no |
| 30 | 10.5 | 0 | 0 | 0 (4,970) | yes |
| 60 | 10.9 | 0 | 0 | 0 (4,970) | yes |
| 120 | 11.0 | 0 | 0 | 0 (4,970) | yes |

## 8. Producer and fill rates

| Input | Events | Fill time | Events/s |
|---|---|---|---|
| Weekday trades (24 Nov) | 59,391,670 | 182.9 s | 324,717 |
| All event types | 289,057,297 | 359.4 s | 804,271 |
| FR / NL / ETR trades | 21.4 M / 6.0 M / 32.0 M | 204.9 / 184.4 / 221.6 s | 104k / 33k / 144k |

The per-exchange fills read every day file in full and keep one exchange, which is why their
rates are lower.

## Figures already made

| File | Shows |
|---|---|
| `results/figures/scalability-throughput-20261124-132305-cores-1-8.png` | throughput vs cores with linear line (in the draft as `scalability-throughput`) |
| `results/figures/scalability-cpu-20261124-132305-cores-1-8.png` | TaskManager CPU vs cores |
| `results/figures/scalability-skew-20261124-132305-cores-1-8.png` | window-operator load per instance |
| `results/figures/latency-vs-rate-c4-*.png`, `latency-cdf-*.png` | latency vs rate, CDFs |
| `results/figures/ooo-late.png`, `ooo-windows.png` | OOO sweep |
| `report/materials/figures/subtask-load.png` | **new**: source vs window load per instance at 4/6/8 cores |
| `report/materials/figures/container-cpu.png` | **new**: CPU per container vs cores |
