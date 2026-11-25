# Design decisions: what we chose, what else was on the table, and the evidence

Raw material for the System Architecture and Implementation sections, and for the rubric's
"creative explorations" line (trade-offs discussed and, ideally, backed by measurements).
These are notes, not report text. Each entry says what the code does (with the file), the
alternatives, the trade-off, and the evidence we have. Pick the ones you want and write them up
in your own words.

Where an alternative was actually tried in this repo, the entry says so and names the commit or
PR. Where it was only considered, it says that too. Don't claim we tried something we didn't.

Quick index:

| # | Decision | Evidence type |
|---|---|---|
| D1 | Kafka + Flink as the stack | team decision log, 29 Sep |
| D2 | Event time, not processing time | answer-key match; Oct prototype already used it |
| D3 | Kafka key = symbol, 8 partitions | per-subtask load measurements |
| D4 | Watermarks per Kafka partition, inside the source | real bug fixed (#SOS); late-trade counts |
| D5 | Out-of-order bound 5 s, idleness 10 s | OOO sweep (real + synthetic), OOO=0 latency run |
| D6 | Start and end markers (#SOS / #EOS) | bug reproduced: 767 false late trades |
| D7 | Classify events at the source, drop non-trades early | router counters; all-events run |
| D8 | Placeholder trading time `01:00:00.000` is dropped | answer key v3, 5,506 trades dropped |
| D9 | Window close = latest trade by (event time, file position) | exact match on 1,855,645 windows |
| D10 | AggregateFunction, not a full window buffer | constant state per window |
| D11 | Two keyBy steps: window operator, then EMA operator | job graph; EMA task 75-95% idle |
| D12 | EMA state = two doubles per symbol; empty windows don't fire | spec + answer key match |
| D13 | First-window advisory kept, but flagged | 4,970 of 11,349 advisories are first-window |
| D14 | Compute every symbol, publish only subscribed ones | spec 3.1 item 3 |
| D15 | Three output topics; late trades go to a side output | late-trades topic, OOO sweep |
| D16 | At-least-once sinks, checkpointing off by default | not measured; a knob exists |
| D17 | Compact CSV records, hand-written parser | Oct prototype used JSON + DateTimeFormatter |
| D18 | Python producer with confluent-kafka | PR #2 note: ~10x faster than kafka-python |
| D19 | Latency knobs exposed (buffer timeout, watermark interval) | tuned vs default latency runs |
| D20 | One TaskManager whose cores and slots are a knob | 1 to 8 core runs |
| D21 | Independent pandas reference + window-by-window validator | every run validated |
| D22 | Everything in Docker Compose, with a self-checking demo | CI on Ubuntu and Windows |
| D23 | Streamlit dashboard design | screenshot, dashboard code |
| D24 | Kafka topic retention set to forever | real bug: 2021 timestamps deleted mid-run |

---

## How the design evolved (useful for the "decisions" and "challenges" answers)

| When | What happened | Where |
|---|---|---|
| 19-29 Sep | Repo set up. Team decision: Docker for infrastructure, Kafka for streaming, Flink for processing | `DECISION.MD` |
| 29 Sep-19 Oct | Data exploration in pandas, long-tail plot, answer key v1 to v3 (v3 adds the placeholder filter), whole-week answer key | `analysis/`, `answer_key/` (Manas) |
| 4 Oct | Producer converts CSV rows to JSON and publishes to `trading-events`; first Flink job deployed | `DECISION.MD`, commits by Yashasvi |
| 17-19 Oct | Kafka to Flink integration; event-time windowing and EMA processing | commits by Yashasvi |
| 1-5 Nov | EMA + signals, Kafka results sink, first Streamlit dashboard, Docker setup with dual Kafka listeners | commits by Yashasvi |
| 4 Nov | Parallelism above 1 failed with `NoResourceAvailableException` (one task slot) | `DECISION.MD` |
| 17-18 Nov | Rework (PRs #2-#4): correctness on the real data, per-partition watermarks, keyed producer, validator, Docker images, experiments, new dashboard | PRs #2, #3, #4 |
| 17 Nov | First Mac experiment run; three script bugs found and fixed (see D24) | `results/RUN-NOTES.md`, PR #5 |
| 24 Nov | Overnight run: 1-8 cores, all events, per exchange, OOO=0 latency | PR #6 |

What the October prototype looked like (commit `d46892d`), so you can describe what changed and why:

- Input topic `trading-events`, records as JSON, no message key, `OffsetsInitializer.latest()`.
- `WatermarkStrategy.forBoundedOutOfOrderness(5 s)` applied **after** the source (one watermark
  generator per source subtask, not per partition).
- Window close via `reduce(...)` keeping the event with the larger timestamp. Ties were not broken
  by file position, so two trades with the same timestamp could pick either one.
- Event time parsed with `DateTimeFormatter` on every event.
- EMA state as separate `ValueState<Double>`s; the same EMA maths as today.
- The producer stamped every row with `08-11-2021` (PR #2), so other days would have looked late.
- Tested on a 1,917-row slice only.

---

## D1. Kafka + Flink

**What we did.** Kafka 3.9 (one broker, KRaft) between the producer and the job; Flink 1.20
DataStream API in Java for the processing.

**Alternatives.**
- Kafka Streams (what last year's sample project used). Runs as a library inside the app, scales
  by running more instances, one per partition group. No separate cluster to run.
- Spark Structured Streaming. Micro-batches by default; event-time windows and watermarks exist,
  but the per-window "last trade" with a tie-break and custom keyed state are less direct.
- A single-process program (Python/pandas or C) on one machine. Simplest and probably fastest on
  one laptop, but it doesn't show distribution or scaling, which the course grades.

**Trade-off notes.** Flink gives event-time windows, per-partition watermarks, side outputs for
late data, keyed state, and operator-level parallelism and metrics through a REST API (which the
experiments read). The cost is a JVM cluster to run (JobManager + TaskManager) and Java code.

**Evidence.** `DECISION.MD` (29 Sep). The REST metrics made the per-subtask load measurements
possible (`results/scalability/*/job-*.json`).

---

## D2. Event time, not processing time

**What we did.** Each trade's timestamp is `Date` + `Trading time`, parsed as if UTC so windows
line up with midnight (`TickParser.java`). Windows are `TumblingEventTimeWindows` of 300,000 ms.

**Alternatives.**
- Processing time (wall clock when Flink sees the event). Last year's project did a version of
  this: its producer replaced the trade time with `Instant.now()`.
- Ingestion time (Kafka record timestamp).
- The other clock in the data, `Time` (when the system received the update).

**Trade-off notes.** With processing time, which window a trade falls in depends on replay speed
and scheduling, so results change between runs and can't be checked against a reference. Event
time is deterministic but needs watermarks, which add latency (D5, timeline diagram).
`Time` was rejected after the data exploration: it is precise only to whole seconds (99.99% end
in `.000`) and on Monday 861 trades have it more than 60 s away from `Trading time`
(`analysis/clocks.py`).

**Evidence.** Exact match with the answer key on every run (1,855,645 windows). Same replay at
different rates (latency runs) gives the same 96,765 windows each time.

---

## D3. Kafka key = symbol, 8 partitions on `ticks`

**What we did.** The producer keys every record by symbol (`producer.py`), so one symbol's
events stay in one partition and in file order. `ticks` has 8 partitions (`PARTITIONS`), the
output topics 1 each.

**Alternatives.**
- No key (round robin), as in the October prototype: spreads load evenly but loses per-symbol
  order, so out-of-order handling would have to do all the work.
- Key by exchange: only 3 keys, so at most 3 useful partitions.
- More partitions than cores (16, 32): a source subtask could take several and the load would
  even out; not tried.
- A custom partitioner for hot symbols (last year's report suggests this as future work).

**Trade-off notes.** Partitions cap source parallelism, and the hash of ~5,000 symbols over 8
partitions is not perfectly even. With 6 source subtasks and 8 partitions, two subtasks read two
partitions each. The window operator is re-keyed by Flink's own hash on the symbol (D11), so its
balance does not depend on Kafka's.

**Evidence** (`facts/derived-facts.md`, figure `figures/subtask-load.png`):
- Partition sizes (8-core run, trades Mon-Fri, million): 9.34, 9.01, 5.91, 7.60, 8.21, 8.31,
  5.81, 5.21. Max/mean 1.26.
- Source subtasks at 6 cores read 15.24, 16.61, 8.21, 8.31, 5.81, 5.21 million: subtasks 0 and 1
  got two partitions each (15.24 = 9.34 + 5.91, 16.61 = 9.01 + 7.60). Max/mean 1.68.
- Window operator after `keyBy`: max/mean 1.10 (4 cores), 1.16 (6), 1.15 (8).

---

## D4. Watermarks per Kafka partition, inside the source

**What we did.** The `WatermarkStrategy` is passed to `env.fromSource(...)`
(`TradingJob.kafkaSource`), so the Kafka source keeps one watermark generator per partition and
emits the minimum. Downstream operators take the minimum over their inputs.

**Alternatives.**
- `assignTimestampsAndWatermarks` after the source (October prototype). One generator per source
  subtask sees several partitions interleaved, so a partition that is ahead pulls the watermark
  forward and the slower partition's trades become late.
- Punctuated watermarks per event: more watermark traffic, no latency benefit at our rates.

**Evidence.** PR #2 (moved to per-partition). On the real Monday, 0 late trades at every bound.

---

## D5. Out-of-order bound 5 s, idleness 10 s

**What we did.** `forBoundedOutOfOrderness(--ooo-ms, default 5000)` and `.withIdleness(--idle-ms,
default 10000)`. Late trades go to a side output (D15).

**Alternatives.** Bound 0 (lowest latency, no tolerance); a large bound (30-120 s, more
tolerance, every window waits longer); allowed lateness with window re-firing (corrections
downstream; the EMA chain would need retractions; not implemented).

**Trade-off notes.** The bound is in event time. It delays every window until trades 5 s of
market time past its end have arrived on every active partition.

**Evidence.**
- Real Monday: 0 late trades and an exact match at 0, 1, 5, 30 and 120 s
  (`results/ooo-sweep/20261117-230210`). The data has no disorder that matters.
- Synthetic, 2% of trades up to 60 s late (`results/ooo-synthetic/`): wrong windows 517 / 229 /
  114 / 114 / 0 / 0 / 0 at 0 / 1 / 5 / 15 / 30 / 60 / 120 s; run time 10.5-12.0 s at every bound.
- Latency with bound 0 (24 Nov) vs 5 s (17 Nov), see `facts/results.md`. Different days, so not a
  clean A/B.

---

## D6. Start and end markers (#SOS, #EOS)

**What we did.** Before a replay the producer sends `#SOS,<ts>` to every partition and pauses 2 s;
after it, `#EOS,<far-future ts>` to every partition (`producer.py`). The parser turns them into
marker events with that event time; the router counts and drops them.

**Why.** #EOS: in a live run the last windows only fire when the watermark passes them, which
needs a later event. #SOS: if the job starts before the producer, all partitions are idle; the
first one to get data drives the watermark alone, and trades arriving on the others a moment
later count as late.

**Alternatives.** Bounded mode only (works for experiments, not for a live system); a processing
time timer that flushes windows (breaks determinism); producer heartbeats (PR #1 had this idea).

**Evidence.** PR #4: 767 trades were counted as late in the synthetic demo before #SOS, 0 after.

---

## D7. Classify events at the source, drop non-trades early

**What we did.** `TickParser` classifies every line (diagram 03); `EventRouter` passes only trades
and counts the rest as Flink metrics (`ticksTrade`, `ticksNonTrade`, `ticksPlaceholder`,
`ticksMalformed`, `ticksStartOfStream`, `ticksEndOfStream`). Quotes get no event time, so they never
move the watermark.

**Alternatives.** Filter in the producer only (the default `--events trades` does this, but then
the job can't be shown to handle the full stream); keep quotes to compute mid prices (not needed
by the queries).

**Evidence.** All-events run: 289,057,297 events in, 59,386,164 trades out of the router,
229,671,133 dropped, same result as the trades-only run.

---

## D8. Placeholder trading time dropped

**What we did.** Trades whose `Trading time` is exactly `01:00:00.000` are counted and dropped
(`TickParser.PLACEHOLDER_TIME`). Same rule as answer key v3.

**Alternative.** Use `Time` (receive time) for those rows instead. Rejected because their real
trade time is unknown and the answer key doesn't do it.

**Evidence.** In the weekday trade runs the router dropped 5,506 trades (FR 4,371, NL 1,135,
ETR 0). These are placeholder-time or malformed rows; the counters in the Flink UI split them.

---

## D9. Window close = latest trade by (event time, file position)

**What we did.** `LastTradeAggregate` keeps the trade with the largest event time; ties go to the
one further down the file (`seq`, sent by the producer). Arrival order doesn't matter.

**Alternative.** Last trade by arrival (what `reduce` effectively did in October): breaks when two
trades share a millisecond timestamp, or when trades arrive out of order within the bound.

**Evidence.** 0 value mismatches over 1,855,645 windows; max relative close error 2.2e-16.

---

## D10. AggregateFunction instead of buffering the window

**What we did.** Incremental aggregation: the accumulator holds last time, seq, price, sec type,
count and max ingest time. `WindowCloseFunction` only adds the window bounds.

**Alternative.** `ProcessWindowFunction` over all elements (Flink keeps every trade of the window
in state until it fires). State would grow with trades per window, and the busiest symbols have
tens of thousands of trades a day.

**Evidence.** Constant state per (symbol, open window). The TaskManager container's memory peaked
at 1.5-2.0 GB in every run (`facts/derived-facts.md`), roughly flat from 2 to 8 cores. That is the
whole JVM, not just window state.

---

## D11. Two keyed operators instead of one

**What we did.** `keyBy(symbol)` into the window operator, then `keyBy(symbol)` again into
`EmaProcessor` (a `KeyedProcessFunction`). Flink runs this as three chained tasks with two hash
shuffles (diagram 02).

**Alternative.** One `KeyedProcessFunction` with event-time timers that tracks the open window
and the EMAs together: one shuffle fewer, more hand-written timer logic.

**Evidence.** The EMA task sees 1,855,645 records against 59.4 M for the window task, and is
75-95% idle in every run (busy 17-25%). Source+router is the busiest task: 84% busy at 8 cores.

---

## D12. EMA state and empty windows

**What we did.** `ValueState<double[]>` = [EMA38, EMA100] per symbol. A window with no trades for
a symbol never fires, so its EMAs carry over unchanged (spec: the EMA updates with "the last
price observed in the window").

**Alternative.** Emit a result for every symbol every 5 minutes, carrying the previous close
forward. That would need timers for ~5,000 symbols and would change the EMA values (the answer key
does not do it).

**Evidence.** State size grows with the number of symbols, not events. Answer-key match.

---

## D13. First-window advisory

**What we did.** With EMA₀ = 0, EMA38 jumps more than EMA100 in a symbol's first window, so the
spec's rule fires a buy there by definition. The job emits it (the spec's equations say so) and
sets `firstWindow = true`; the dashboard hides these by default.

**Alternative.** Suppress them. PR #1 (closed without merging) proposed this; it disagrees with the
spec's formulas.

**Evidence.** 11,349 advisories, of which 4,970 are first-window (one per symbol); the other
6,379 also all match.

---

## D14. Compute everything, publish only subscribed symbols

**What we did.** Every symbol's EMAs are computed. A filter before the sinks publishes only
symbols in `--subscriptions` (26 in `config/subscriptions.txt`: the 4 busiest per exchange, the 8
with most crossovers, 6 long-tail ones).

**Alternative.** Filter subscriptions at the source: less work, but a new subscriber would start
from a cold EMA (and the spec says all metrics are calculated for all symbols).

**Evidence.** Spec 3.1 item 3. Experiments run with `--subscriptions all` so validation sees
everything.

---

## D15. Three output topics, late side output

**What we did.** `ema-results` (Query 1, every window), `advisories` (Query 2, buy/sell only),
`late-trades` (trades that arrived after their window fired). JSON for results, CSV for late
trades.

**Alternative.** One topic with everything (October prototype); silently dropping late data
(Flink's default).

---

## D16. Delivery guarantees

**What we did.** Kafka sinks are `AT_LEAST_ONCE`; checkpointing is off unless `--checkpoint-ms`.

**Alternative.** `EXACTLY_ONCE` sinks with transactions and checkpointing on.

**Trade-off notes.** Exactly-once adds checkpoint barriers and transactional commits (latency
grows to the checkpoint interval for consumers reading committed data). Not measured here. Say
what you think it would cost, or measure it if there's time.

---

## D17. Compact CSV records and a hand-written parser

**What we did.** Records are `ID,SecType,Date,Time,Trading time,Last,seq`. `TickParser` parses
times by hand and caches the date, because `DateTimeFormatter` per event is slow at hundreds of
thousands of events per second.

**Alternative.** JSON per event with Jackson (October prototype), Avro with a schema registry
(last year's future work).

**Evidence.** Not benchmarked separately. Producer fills at 325k events/s; the job does 0.87M
events/s on one core.

---

## D18. Python producer with confluent-kafka

**What we did.** pandas chunked reader (`tools/ticks.py`, shared with the reference so both see
the same events), librdkafka with `linger.ms=20`, 10,000-message batches, lz4, `acks=1`.

**Alternative.** kafka-python (PR #2 says about 10x slower), a JVM producer (last year: Scala with
a synchronous `send().get()` per record).

**Evidence.** 325k events/s for trades, 804k events/s with all events (more rows per chunk).

---

## D19. Latency knobs

**What we did.** `--buffer-timeout-ms` (Flink default 100) and `--watermark-interval-ms`
(default 200) are job options.

**Evidence.** Tuned (5 / 50 ms) vs default at 4 cores: p50 93 vs 266 ms at 100k events/s. Numbers
in `facts/results.md`. Throughput cost of the tuned settings: not measured.

---

## D20. One TaskManager, cores and slots as a knob

**What we did.** `TM_CPUS` (Docker CPU cap) and `TM_SLOTS`; the experiments restart the
TaskManager for each core count and set parallelism = cores.

**Alternative.** Several TaskManagers (closer to a cluster, more network between them); a real
multi-machine cluster (not available).

**Evidence.** The October setup had 1 slot (`NoResourceAvailableException` above 1, 4 Nov in
`DECISION.MD`). Now 1-8 cores all run.

---

## D21. Independent reference and validator

**What we did.** `tools/reference.py` (pandas, same rules as the answer key) and
`tools/validate.py` (window-by-window: missing, extra, value errors, missed / spurious /
wrong-direction advisories). The week answer key is `answer_key/answer-key-week.parquet`.

**Alternative.** Spot checks of a few symbols (what last year did, on dummy data with 1-minute
windows).

**Evidence.** Every experiment run validated; CI runs the check on every push.

---

## D22. Docker Compose and a self-checking demo

**What we did.** Two images (`ssdm-flink:1.0`, `ssdm-python:1.0`), profiles `demo` and `app`,
a `verify` service that exits 0 or 1. CI runs the demo on Ubuntu and the quick start on Windows.

---

## D23. Dashboard

**What we did.** Streamlit + Altair, live from Kafka or from a file. Advisory feed (first-window
buys hidden), every subscribed symbol sorted by EMA38 distance from EMA100 as % of EMA100
(comparable across prices), a per-symbol chart with markers, an advisory timeline. Symbols with
fewer than 100 windows are listed as warming up.

**Alternatives.** Grafana on a time-series DB (last year's future work), a custom web app with
WebSockets (last year's approach). Streamlit was quicker and reads Kafka directly.

---

## D24. Topic retention forever

**What we did.** `retention.ms=-1` on all topics.

**Why.** Flink writes output records with their event-time timestamp (November 2021). With
Kafka's default 7-day retention the broker deleted `ema-results` segments during a run: the
20k events/s latency runs lost results (5,891 and 83,149 of 96,765 windows dumped) on 17 Nov.

**Evidence.** `results/RUN-NOTES.md`, "Changes to the scripts", item 3.
