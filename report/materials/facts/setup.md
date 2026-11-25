# Hardware, software and configuration

For the Implementation section ("hardware/cloud services, programming tools, and GUI tools") and
the experimental setup. Every value is from a file in the repo; the file is named.

## Hardware

| Item | Value | Source |
|---|---|---|
| Machine | MacBook Air, Apple M5, 10 cores (`hw.ncpu`) | `results/RUN-NOTES.md` |
| Performance vs efficiency cores | **not recorded yet.** Check in System Information (or `sysctl hw.perflevel0.physicalcpu hw.perflevel1.physicalcpu`) | open TODO in `report.tex` |
| Memory | 16 GB | `RUN-NOTES.md` |
| Cooling / power | passively cooled (no fan), on mains power | `RUN-NOTES.md`, `experiments/README.md` |
| Docker Desktop | 29.8.0, 10 CPUs, 7.75 GiB (8,319,504,384 bytes) | `RUN-NOTES.md`, `settings.json` |
| Host OS seen by the scripts | Darwin arm64 | `settings.json` |
| Synthetic OOO sweep and cloud preview | 4-core x86-64 Linux cloud container | `results/ooo-synthetic/README.md`, `results/preview-cloud/` |

## Software

| Component | Version | Source |
|---|---|---|
| Apache Kafka | 3.9.0 (`apache/kafka:3.9.0`), one node in KRaft mode (broker + controller) | `docker-compose.yml` |
| Apache Flink | 1.20.0 (`flink:1.20.0-java17`), Kafka connector 3.3.0-1.20 | `flink-job/Dockerfile`, `pom.xml` |
| Job language / build | Java (compiled for 11, runs on 17), Maven 3.9.9 in the image build | `pom.xml`, `flink-job/Dockerfile` |
| Python | 3.11 in the image (`python:3.11-slim`); 3.14.7 venv on the Mac | `Dockerfile`, `RUN-NOTES.md` |
| Local Java / Maven on the Mac | openjdk 17.0.21, Maven 3.10.1 | `RUN-NOTES.md` |
| Kafka client (producer, dashboard) | confluent-kafka (librdkafka) | `requirements.txt` |
| Dashboard | Streamlit + Altair | `dashboard/dashboard.py` |
| Reference, validator, analysis | pandas, NumPy, matplotlib | `tools/`, `experiments/analyze.py` |
| CI | GitHub Actions: unit tests, test slice, synthetic stress test, Docker demo on Ubuntu, quick start on Windows | `.github/workflows/ci.yml` |

## Containers (docker-compose.yml)

| Service | Image | Resources / ports | Role |
|---|---|---|---|
| kafka | apache/kafka:3.9.0 | 9092 (host), 29092 (inside Docker) | broker + controller |
| kafka-init | apache/kafka:3.9.0 | runs once | creates topics |
| jobmanager | ssdm-flink:1.0 | 1 GB process memory, UI/REST 8081 | schedules the job |
| taskmanager | ssdm-flink:1.0 | `TM_CPUS` cores (Docker cap; 0 = no cap), `TM_SLOTS` slots (default 4), 3 GB process memory | runs the job |
| job | ssdm-flink:1.0 | runs once | `flink run -d -p $PARALLELISM` (default 4) |
| producer | ssdm-python:1.0 | mounts `./data` read-only | replays the week (`app` profile) |
| demo-producer, verify | ssdm-python:1.0 | | demo replay and self-check (`demo` profile) |
| dashboard | ssdm-python:1.0 | 8501 | Streamlit |

## Kafka topics

| Topic | Partitions | Key | Value | Retention |
|---|---|---|---|---|
| `ticks` | 8 (`PARTITIONS`) | symbol | `ID,SecType,Date,Time,Trading time,Last,seq` | forever (`-1`) |
| `ema-results` | 1 | none | JSON, one per symbol and window (subscribed symbols) | forever |
| `advisories` | 1 | none | JSON, buy/sell only | forever |
| `late-trades` | 1 | none | `symbol,event_ts,price,seq` | forever |

Output JSON fields: `symbol, secType, windowStart, windowEnd, close, trades, ema38, ema100,
firstWindow, signal, maxIngestTs, firedTs, emitTs` (times in epoch ms; the last three are wall clock).

## Producer settings (`producer/producer.py`)

`linger.ms=20`, `batch.num.messages=10000`, queue up to 1,000,000 messages / 1 GB,
`compression.type=lz4`, `acks=1`. Pacing: `--rate` events/s or `--speed` x market time, checked
every 2,000 events. `#SOS` to every partition then a 2 s pause; `#EOS` to every partition at the end.
Fill rates measured: 325k events/s (trades), 804k events/s (all events).

## Flink job settings (`TradingJob.java`)

| Setting | Default | Option |
|---|---|---|
| Window | tumbling event time, 300,000 ms, aligned to midnight | |
| Out-of-order bound | 5,000 ms | `--ooo-ms` |
| Idleness | 10,000 ms | `--idle-ms` |
| Network buffer timeout | Flink default 100 ms (tuned run: 5) | `--buffer-timeout-ms` |
| Watermark interval | Flink default 200 ms (tuned run: 50) | `--watermark-interval-ms` |
| Checkpointing | off | `--checkpoint-ms` |
| Sink guarantee | at-least-once, `linger.ms=5` | |
| Start offsets | earliest | `--start` |
| Bounded mode | off (experiments use `--bounded`) | `--bounded` |
| Max parallelism | 128 (Flink default, seen in the REST output) | |
| Slot sharing | one group: one slot runs one subtask of every task | REST output |

## How "cores" is varied in the experiments

`taskmanager_with_cores` in `experiments/common.sh`: restart the TaskManager with Docker `cpus = c`
and `c` slots, wait until Flink reports exactly `c` slots and 1 TaskManager, submit with `-p c`.
Kafka, the JobManager and Docker itself share the remaining cores of the same 10-CPU VM. The
producer fills the topic before the measured runs and is idle during them.
