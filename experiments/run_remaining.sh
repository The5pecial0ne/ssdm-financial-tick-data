#!/usr/bin/env bash
# The runs the evaluation still needs, chained so they can run unattended (overnight):
#
#   1. more cores: all trades of the week at 1, 2, 4, 6 and 8 cores, so the curve goes past 4
#      (the course suggested showing 2, 4, 6... workers)
#   2. every event type, not just trades: the whole working week, about 289 million events
#      instead of 59 million, at 1, 2 and 4 cores, checked against the answer key afterwards
#   3. each exchange on its own (Paris, Amsterdam, Frankfurt): all trades of the week, 1, 2 and 4
#      cores, each checked against a reference computed for that exchange
#   4. latency with no out-of-order slack (bound 0 instead of 5 s), the latency side of the
#      trade-off that results/ooo-synthetic/ measures on the correctness side
#
# Then it regenerates results/summary.md and the figures from every scalability and latency run
# (old and new) and the newest out-of-order sweep, with a table comparing the scalability runs.
#
#   ./experiments/run_remaining.sh
#
# Needs the same setup as run_scalability.sh (Docker with 10 CPUs and about 8 GB, the data in data/).
# Expect 1 to 3 hours on a MacBook: most of it is the producer reading the CSV files again for
# each run. Kafka needs about 10 GB of free disk in Docker for step 2.
# Settings: REPEATS=1 (runs per core count)
#           SKIP_MORE_CORES=1 / SKIP_ALL_EVENTS=1 / SKIP_EXCHANGES=1 / SKIP_LATENCY=1 to leave a step out
set -euo pipefail
cd "$(dirname "$0")/.."
REPEATS=${REPEATS:-1}
stamp=$(date +%Y%m%d-%H%M%S)

if [[ -z "${SKIP_MORE_CORES:-}" ]]; then
  OUT=results/scalability/$stamp-cores-1-8 CORES="1 2 4 6 8" REPEATS=$REPEATS ./experiments/run_scalability.sh
fi
if [[ -z "${SKIP_ALL_EVENTS:-}" ]]; then
  OUT=results/scalability/$stamp-all-events EVENTS=all REPEATS=$REPEATS ./experiments/run_scalability.sh
fi
if [[ -z "${SKIP_EXCHANGES:-}" ]]; then
  for ex in FR NL ETR; do
    OUT=results/scalability/$stamp-$ex EXCHANGES=$ex REPEATS=$REPEATS ./experiments/run_scalability.sh
  done
fi
if [[ -z "${SKIP_LATENCY:-}" ]]; then
  OUT=results/latency/$stamp-ooo0 OOO_MS=0 ./experiments/run_latency.sh
fi

latency_runs=()
for d in results/latency/*/; do
  [[ -f "$d/summary.csv" || -f "$d/settings.json" ]] && latency_runs+=("${d%/}")
done
python3 -m experiments.analyze --scalability results/scalability/*/ \
  ${latency_runs[@]+--latency "${latency_runs[@]}"}
echo "done. New tables are in results/summary.md, figures in results/figures/"
