# Running and checking the project on another machine

This guide is for anyone who wants to run the system somewhere other than the machine it was built
on: a grader, a teammate, or a clean Linux or Windows PC. Every path below ends in the same automatic
check. Flink's output is compared, window by window, with an independent pandas reference, and the
run ends with `PASSED` or `FAILED`.

| You want to | Do this | Takes |
|---|---|---|
| See it working, without downloading the dataset | [1. The demo](#1-the-demo-recommended) | about 10 minutes the first time, then 1 minute |
| Run it without building anything | [2. Prebuilt images](#2-run-prebuilt-images-instead-of-building) | a few minutes |
| Run the real week of data | [3. The full week](#3-the-full-week-of-real-data) | an hour or more, plus a download of about 25 GB |
| Check correctness without Docker | [4. Without Docker](#4-without-docker-java-and-python-only) | 2 minutes |

GitHub Actions runs path 1 on a clean Ubuntu machine, and path 4 on Ubuntu and on Windows, for every
push (`.github/workflows/ci.yml`). A green tick there means those steps worked from scratch.

## What you need

- **Docker**
  - Windows or macOS: [Docker Desktop](https://www.docker.com/products/docker-desktop/). On Windows it uses WSL 2, which the installer sets up.
  - Linux: Docker Engine with the Compose plugin (`docker compose version` should print v2).
- **Memory and CPU for Docker:** about 6 GB of memory and 4 CPUs. Docker Desktop on macOS sets this in
  Settings → Resources. On Windows, WSL 2 gives Docker half the machine's memory by default, which is
  enough on a 16 GB PC. On an 8 GB PC, see [Troubleshooting](#troubleshooting).
- **Disk:** about 6 GB free for the images.
- **Free ports:** 8081 (Flink UI), 8501 (dashboard) and 9092 (Kafka).
- **Git**, to get the code. On Windows that's [Git for Windows](https://git-scm.com/download/win), which
  also gives you Git Bash for path 4.

Get the code:

```bash
git clone https://github.com/The5pecial0ne/ssdm-financial-tick-data.git
cd ssdm-financial-tick-data
```

A ZIP download of the repository works as well. The scripts keep Unix line endings on Windows
(`.gitattributes` takes care of that), so you don't need to change any Git settings.

## 1. The demo (recommended)

The demo needs nothing beyond the repository and Docker. It builds the images and starts Kafka and a Flink
cluster. It then replays a real excerpt of the DEBS 2022 data through Kafka into Flink and checks
what Flink published against the reference. It also starts the dashboard.

**Linux, macOS, or Windows (PowerShell, Command Prompt or Git Bash):**

```bash
docker compose --profile demo up -d --build
```

The first build takes 5 to 10 minutes, most of it downloading Flink, Maven and the Python packages.
The command returns once the replay has finished and the check has started. Then:

```bash
docker compose --profile demo logs -f verify
```

After about half a minute it prints the comparison and ends like this:

```
windows: reference 330 | Flink 330 | matched 330 | missing 0 | extra 0
values: 0 windows differ beyond 1e-09 relative; max relative error {'close': 0.0, 'ema38': 0.0, 'ema100': 0.0}
signals_all: {'expected': 3, 'matched': 3, 'missed': 0, 'spurious': 0, 'wrong_direction': 0}
signals_excluding_first_windows: {'expected': 1, 'matched': 1, 'missed': 0, 'spurious': 0, 'wrong_direction': 0}
PASSED -> /demo/flink.validation.json
Dashboard: http://localhost:8501   Flink UI: http://localhost:8081
```

Press Ctrl+C to stop following the log. The system keeps running.

- **Dashboard:** http://localhost:8501. The advisory feed, the trend of every symbol, and one symbol's
  price with its EMA38, EMA100 and buy/sell points. The sidebar switches between live (Kafka) and a
  results file.
- **Flink UI:** http://localhost:8081. The running job, its operators, and their metrics
  (`ticksTrade`, `ticksNonTrade`, `ticksPlaceholder` and so on).

### A bigger demo: three synthetic days

The real excerpt covers two symbols. For a heavier run, the demo can generate three trading days
in the same CSV layout: 2.1 million events, thousands of symbols with a long-tail popularity, quotes,
placeholder times and trades that arrive out of order (`tools/synth.py`).

```bash
docker compose --profile demo down --volumes       # start from empty topics

# Linux, macOS, Git Bash
DEMO_INPUT=synthetic docker compose --profile demo up -d

# Windows PowerShell
$env:DEMO_INPUT = "synthetic"; docker compose --profile demo up -d; Remove-Item Env:DEMO_INPUT

docker compose --profile demo logs -f verify
```

This takes a few minutes and ends with `PASSED`, with about 5,500 windows of the subscribed symbols
and about 50 advisories, all matching the reference. The data is generated on the spot, so the exact
counts can differ a little between machines with different library versions.

### Stop and clean up

```bash
docker compose --profile demo down --volumes
```

Always use `down --volumes` before running the demo again. Otherwise Kafka still holds the previous
run's records, and the check compares against the wrong input.

## 2. Run prebuilt images instead of building

There are two ways to get the images without building them.

### From a file (offline, no Docker Hub)

On the machine that built the images (for example after running the demo once):

```bash
./scripts/export-images.sh                         # builds, then writes ssdm-images.tar.gz (about 1 GB)
NO_BUILD=1 ./scripts/export-images.sh              # uses the images you already built
PLATFORM=linux/amd64 ./scripts/export-images.sh    # on an Apple Silicon Mac, for an ordinary PC
```

The images have to match the other machine's CPU type. Images built on an Apple Silicon Mac are for
ARM, and an ordinary Windows or Linux PC needs `PLATFORM=linux/amd64`. That build is slower, and the
script rebuilds the Mac's own images at the end, so the Mac's experiments still run natively.

On the other machine, put the file in the repository folder and run:

```bash
docker load -i ssdm-images.tar.gz
docker compose --profile demo up -d --no-build
docker compose --profile demo logs -f verify
```

### From GitHub's container registry

This only works once the images have been published with the manual "publish images" workflow in
the repository's Actions tab (it builds them for both PCs and Apple Silicon). The repository's
**Packages** list on GitHub shows whether `ssdm-flink` and `ssdm-python` are there. If they aren't,
`docker compose pull` fails with "denied"; build them instead with the demo in section 1. If they
are, set `IMAGE_PREFIX` to the registry path:

```bash
# Linux, macOS, Git Bash
export IMAGE_PREFIX=ghcr.io/the5pecial0ne/
docker compose --profile demo pull
docker compose --profile demo up -d --no-build

# Windows PowerShell
$env:IMAGE_PREFIX = "ghcr.io/the5pecial0ne/"
docker compose --profile demo pull
docker compose --profile demo up -d --no-build
```

## 3. The full week of real data

1. Download the trading data from Zenodo, [record 6382482](https://doi.org/10.5281/zenodo.6382482),
   and put the day files in `data/` as `data/debs2022-gc-trading-day-08-11-21.csv` and so on, up to
   `14-11-21` (the main README has the details). The files take about 25 GB, and Kafka needs
   another 10 GB or so inside Docker while the week is replayed.
2. Start everything. The producer replays Monday to Friday as fast as it can:

   ```bash
   docker compose --profile app up -d --build
   docker compose --profile app logs -f producer      # progress; it exits when the week is sent
   ```

   To watch it in market time instead, add `SPEED=60` (an hour of trading per minute) in front of
   the command, or in PowerShell set `$env:SPEED = "60"` first.
3. Once the producer has finished, check the published results against the shipped answer key for
   the whole week:

   ```bash
   docker compose run --rm --no-deps dashboard bash -c "python -m tools.dump_topic ema-results /tmp/week.jsonl --bootstrap kafka:29092 --idle 30 && python -m tools.validate --flink /tmp/week.jsonl --answer-key answer_key/answer-key-week.parquet --symbols-from-flink"
   ```

   This job publishes the 26 symbols in `config/subscriptions.txt`, so the check covers those. The
   scalability experiment checks all 4,970 symbols (`experiments/README.md`).
4. Stop with `docker compose --profile app down --volumes`.

## 4. Without Docker (Java and Python only)

This runs the same Flink job in an in-process cluster, reading the CSV directly, so it shows the
results are right without Kafka or Docker. You need Java 11 or newer, Maven and Python 3.10 or newer.

**Linux and macOS:**

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
./flink-job/run-local.sh analysis/test-slice.csv results/test-slice.jsonl
python -m tools.validate --flink results/test-slice.jsonl --csv analysis/test-slice.csv
```

**Windows, in Git Bash** (install Java 17, for example [Temurin](https://adoptium.net/), plus
[Maven](https://maven.apache.org/download.cgi) and [Python](https://www.python.org/downloads/windows/)):

```bash
python -m venv venv && source venv/Scripts/activate
pip install -r requirements.txt
./flink-job/run-local.sh analysis/test-slice.csv results/test-slice.jsonl
python -m tools.validate --flink results/test-slice.jsonl --csv analysis/test-slice.csv
```

Both end with `PASSED`. For a harder test, generate synthetic data first:

```bash
python -m tools.synth -n 3000000 -o data/synthetic.csv
./flink-job/run-local.sh data/synthetic.csv results/synthetic.jsonl --parallelism 4
python -m tools.validate --flink results/synthetic.jsonl --csv data/synthetic.csv
```

## Troubleshooting

- **"port is already allocated"**: something else uses 8081, 8501 or 9092. Stop it, or stop an old
  run of this project with `docker compose --profile demo down --volumes`.
- **"container name ... is already in use"**: a container called `kafka`, `jobmanager` or
  `taskmanager` is left over, maybe from another project. Remove it with `docker rm -f kafka jobmanager taskmanager`.
- **`toomanyrequests` while building**: Docker Hub limits anonymous downloads. Run `docker login`
  (a free account is enough), or use the image file from section 2.
- **The check fails right after an earlier run**: run `docker compose --profile demo down --volumes`
  first, so the topics start empty.
- **Only 8 GB of memory in total**: give the TaskManager less, for example
  `TM_MEMORY=1500m docker compose --profile demo up -d --build` (PowerShell: `$env:TM_MEMORY = "1500m"` first).
  The synthetic demo passes like that, with the whole system using under 2 GB.
- **Linux: "permission denied ... docker.sock"**: add yourself to the `docker` group
  (`sudo usermod -aG docker $USER`, then log out and in), or put `sudo` in front of the commands.
- **Windows: `docker` is not found in Git Bash**: start Docker Desktop first and open a new terminal.
- **Windows: `/usr/bin/env: 'bash\r'`**: a script got Windows line endings, for example from an
  editor or a ZIP tool. A fresh `git clone` restores the right ones.
- **Something else**: `docker compose --profile demo logs --tail 100` shows what every service printed.
