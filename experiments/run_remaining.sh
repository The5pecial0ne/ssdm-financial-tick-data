#!/usr/bin/env bash
# The scalability runs the evaluation still needs, chained so they can run unattended (overnight):
#
#   1. every event type, not just trades: the whole working week, about 289 million events
#      instead of 59 million, at 1, 2 and 4 cores, checked against the answer key afterwards
#   2. each exchange on its own (Paris, Amsterdam, Frankfurt): all trades of the week, 1, 2 and 4
#      cores, each checked against a reference computed for that exchange
#
# Then it regenerates results/summary.md and the figures from every scalability run (old and new),
# plus the two latency runs and the newest out-of-order sweep, with a table comparing all the runs.
#
#   ./experiments/run_remaining.sh
#
# Needs the same setup as run_scalability.sh (Docker with 10 CPUs and about 8 GB, the data in data/).
# Expect roughly 3 hours on a MacBook: most of it is the producer reading the CSV files again for
# each run. Kafka needs about 10 GB of free disk in Docker for step 1.
# Settings: REPEATS=1 (runs per core count)  SKIP_ALL_EVENTS=1 / SKIP_EXCHANGES=1 to leave a step out
set -euo pipefail
cd "$(dirname "$0")/.."
REPEATS=${REPEATS:-1}
stamp=$(date +%Y%m%d-%H%M%S)

if [[ -z "${SKIP_ALL_EVENTS:-}" ]]; then
  OUT=results/scalability/$stamp-all-events EVENTS=all REPEATS=$REPEATS ./experiments/run_scalability.sh
fi
if [[ -z "${SKIP_EXCHANGES:-}" ]]; then
  for ex in FR NL ETR; do
    OUT=results/scalability/$stamp-$ex EXCHANGES=$ex REPEATS=$REPEATS ./experiments/run_scalability.sh
  done
fi

latency_runs=()
for d in results/latency/*/; do
  [[ -f "$d/summary.csv" || -f "$d/settings.json" ]] && latency_runs+=("${d%/}")
done
python3 -m experiments.analyze --scalability results/scalability/*/ \
  ${latency_runs[@]+--latency "${latency_runs[@]}"}
echo "done. New tables are in results/summary.md, figures in results/figures/"
