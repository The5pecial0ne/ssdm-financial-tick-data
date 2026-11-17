#!/usr/bin/env bash
# Scalability without Docker, for Linux machines (it pins the JVM to N cores with taskset).
# It runs the bounded job in Flink's in-process mini cluster against a Kafka broker on
# localhost:9092 that you've already filled with producer/producer.py. The output has the same
# layout as run_scalability.sh, so experiments/analyze.py reads both.
#
#   CORES="1 2 4" REPEATS=3 ./experiments/run_local_preview.sh
#
# On a Mac use run_scalability.sh (Docker CPU limits) instead; macOS has no taskset.
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"
PY=${PY:-python3}
CORES=${CORES:-"1 2 4"}
REPEATS=${REPEATS:-3}
BOOTSTRAP=${BOOTSTRAP:-localhost:9092}
OUT=${OUT:-results/scalability/$(date +%Y%m%d-%H%M%S)-local}
mkdir -p "$OUT"
command -v taskset >/dev/null || { echo "needs taskset (Linux)"; exit 1; }
[[ -f "${PRODUCER_STATS:-}" ]] || { echo "set PRODUCER_STATS to the --stats file of the producer run that filled the topic"; exit 1; }
cp "$PRODUCER_STATS" "$OUT/producer.json"

cat > "$OUT/settings.json" <<JSON
{"cores": "$CORES", "repeats": $REPEATS, "days": "${DAYS_LABEL:-see producer.json}", "events": "${EVENTS_LABEL:-see producer.json}",
 "host": "$(uname -sm), $(nproc) cores, mini cluster pinned with taskset", "docker_cpus": "n/a"}
JSON

echo "cores,repeat,job_id,state,run_ms" > "$OUT/runs.csv"
for c in $CORES; do
  for r in $(seq 1 "$REPEATS"); do
    echo "== $c core(s), repeat $r"
    log="$OUT/run-c$c-r$r.log"
    # The results topic is per run so runs never mix
    taskset -c "0-$((c - 1))" ./flink-job/run-local.sh -- --bootstrap "$BOOTSTRAP" --bounded \
      --parallelism "$c" --group "preview-c$c-r$r-$RANDOM" --results-topic "preview-c$c-r$r" \
      --advisories-topic "preview-adv-c$c-r$r" > "$log" 2>&1 &
    pid=$!
    # CPU of the JVM over each second, from /proc (percent of one core, like docker stats)
    echo "wall_ms,container,cpu_percent,mem" > "$OUT/cpu-c$c-r$r.csv"
    hz=$(getconf CLK_TCK); prev=""
    while kill -0 $pid 2>/dev/null; do
      jpid=$(pgrep -f "ssdm.TradingJob.*preview-c$c-r$r" | head -1 || true)
      if [[ -n "$jpid" && -r /proc/$jpid/stat ]]; then
        ticks=$(awk '{print $14+$15}' /proc/$jpid/stat 2>/dev/null || echo "")
        if [[ -n "$prev" && -n "$ticks" ]]; then
          echo "$(date +%s%3N),taskmanager,$(( (ticks - prev) * 100 / hz ))," >> "$OUT/cpu-c$c-r$r.csv"
        fi
        prev=$ticks
      fi
      sleep 1
    done
    wait $pid && state=FINISHED || state=FAILED
    ms=$(sed -n 's/^Net runtime ms: \([0-9]*\)/\1/p' "$log" | tail -1)
    echo "$c,$r,local,$state,${ms:--1}" >> "$OUT/runs.csv"
    echo "   $state in ${ms:-?} ms"
  done
done
echo "done: $OUT"
