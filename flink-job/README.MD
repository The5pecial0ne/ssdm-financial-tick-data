# Flink job

The stream-processing job: Query 1 (EMA38 and EMA100 per symbol over five-minute event-time windows)
and Query 2 (buy and sell advisories when the two cross). Flink 1.20.0, DataStream API, compiled for
Java 11 and run on Java 17.

## Code (`src/main/java/ssdm/`)

| Class | Role |
|---|---|
| `TradingJob` | Reads the options, builds the pipeline (Kafka or file source, watermarks, two keyed operators, sinks) |
| `TickDeserializer`, `TickParser`, `TickEvent` | Read a record and classify it as trade, quote, placeholder or malformed |
| `EventRouter` | Passes trades on and counts the other kinds as Flink metrics |
| `LastTradeAggregate`, `WindowCloseFunction`, `WindowClose` | The five-minute window and its close (latest trading time, ties broken by file position) |
| `EmaProcessor`, `Ema`, `EmaResult` | Keyed EMA state, the EMA update and the crossover check |
| `Subscriptions` | Filters what is published to the subscribed symbols |

`src/test/java/ssdm/CoreLogicTest.java` holds unit tests for the parser, the EMA maths and the crossover rules.

## Build and run

```bash
mvn -B package            # in flink-job/: builds target/flink-job-1.0.jar and runs the unit tests
./flink-job/run-local.sh analysis/test-slice.csv results/test-slice.jsonl   # from the repository root
```

`run-local.sh` runs the job in an in-process Flink cluster on a CSV file. In Docker, the image built from
`Dockerfile` is started by `docker-compose.yml` (see the main README and `docs/TESTING.md`).

Main options: `--input-topic` or `--input-file`, `--results-topic`, `--advisories-topic`, `--late-topic`,
`--subscriptions`, `--parallelism`, `--ooo-ms` (out-of-order bound, default 5000), `--idle-ms`,
`--buffer-timeout-ms`, `--watermark-interval-ms`, `--checkpoint-ms` and `--bounded`.
