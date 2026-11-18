#!/usr/bin/env bash
# Design exploration: the out-of-order bound trades latency for correctness.
#
# A small bound means windows fire sooner, but trades that arrive later than the bound
# miss their window (they go to the late-trades output), so some closes, EMAs and advisories
# differ from the answer key. A large bound fixes that but every result waits longer.
# This runs the job in file mode (no Docker needed) on one real day for several bounds and
# validates each run against the pandas reference for the same day.
#
#   ./experiments/run_ooo_sweep.sh
#
# Settings: DAY=08-11-21  BOUNDS="0 1000 5000 30000 120000" (ms)  PARALLELISM=4
#           CSV=<file> to sweep another input instead of the real day, for example synthetic data with
#           a known amount of disorder (that's how results/ooo-synthetic/ was made):
#   python -m tools.synth -n 3000000 --out-of-order 0.02 --max-delay-ms 60000 -o data/synthetic-ooo.csv
#   CSV=data/synthetic-ooo.csv OUT=results/ooo-synthetic/ooo-sweep/run \
#     BOUNDS="0 1000 5000 15000 30000 60000 120000" ./experiments/run_ooo_sweep.sh
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"
PY=${PY:-python3}
DAY=${DAY:-08-11-21}
BOUNDS=${BOUNDS:-"0 1000 5000 30000 120000"}
PARALLELISM=${PARALLELISM:-4}
OUT=${OUT:-results/ooo-sweep/$(date +%Y%m%d-%H%M%S)}
CSV=${CSV:-data/debs2022-gc-trading-day-$DAY.csv}
mkdir -p "$OUT"
printf '{"input": "%s", "bounds_ms": "%s", "parallelism": %s, "host": "%s", "cpus": %s}\n' \
  "$CSV" "$BOUNDS" "$PARALLELISM" "$(uname -sm)" "$(getconf _NPROCESSORS_ONLN)" > "$OUT/settings.json"
[[ -f "$CSV" ]] || { echo "missing $CSV (see README, 'Get the data')"; exit 1; }

echo "computing the reference for $(basename "$CSV") once ..."
$PY -m tools.reference --file "$CSV" -o "$OUT/reference.parquet"

echo "bound_ms,seconds,late_trades,passed" > "$OUT/sweep.csv"
for b in $BOUNDS; do
  echo "== out-of-order bound $b ms"
  start=$($PY -c 'import time; print(time.time())')
  ./flink-job/run-local.sh "$CSV" "$OUT/flink-ooo$b.jsonl" --ooo-ms "$b" --parallelism "$PARALLELISM" \
    2>&1 | grep -E "Wrote|Exception" || true
  secs=$($PY -c "import time; print(round(time.time()-$start,1))")
  late=$(($(wc -l < "$OUT/flink-ooo$b.late.csv") - 1))
  if $PY -m tools.validate --flink "$OUT/flink-ooo$b.jsonl" --reference "$OUT/reference.parquet" \
       --report "$OUT/validation-ooo$b.json" | tail -4; then ok=1; else ok=0; fi
  echo "$b,$secs,$late,$ok" >> "$OUT/sweep.csv"
done
cat "$OUT/sweep.csv"
echo "done: $OUT  (next: python -m experiments.analyze)"
