#!/usr/bin/env bash
# Scalability: how long the job takes to process the same data with 1, 2 and 4 cores.
#
# The input topic is filled once; then for every core count the TaskManager is restarted with
# that many cores and slots and a *bounded* job (stops when it has read everything) is run at
# that parallelism. Processing time comes from Flink's REST API, CPU usage from docker stats.
# The producer is idle during the measured runs, so it never competes for CPU.
#
#   ./experiments/run_scalability.sh
#
# Settings (environment variables):
#   CORES="1 2 4"        core counts to test          REPEATS=3      runs per core count
#   DAYS=weekdays        days to replay (see producer) EVENTS=trades  or "all" for every event type
#   EXCHANGES=           e.g. FR to test one exchange  FROM= / TO=    time-of-day range, e.g. 08:00 / 12:00
#   JOB_ARGS=            extra job options, e.g. "--buffer-timeout-ms 5"
#   VALIDATE=1           compare the last run's output with the reference (the shipped answer key for
#                        full weekdays, otherwise a reference computed for the same slice of the data)
#   OUT=results/scalability/<timestamp>
source "$(dirname "$0")/common.sh"

CORES=${CORES:-"1 2 4"}
REPEATS=${REPEATS:-3}
DAYS=${DAYS:-weekdays}
EVENTS=${EVENTS:-trades}
VALIDATE=${VALIDATE:-1}
OUT=${OUT:-results/scalability/$(date +%Y%m%d-%H%M%S)}
mkdir -p "$OUT"

max_cores=$(echo "$CORES" | tr ' ' '\n' | sort -n | tail -1)
stack_up "$max_cores"

log "filling the input topic: days=$DAYS events=$EVENTS exchanges=${EXCHANGES:-all} ${FROM:-} ${TO:-}"
reset_input
args=(--days "$DAYS" --events "$EVENTS" --stats "$OUT/producer.json" --quiet)
[[ -n "${EXCHANGES:-}" ]] && args+=(--exchanges "$EXCHANGES")
[[ -n "${FROM:-}" ]] && args+=(--from "$FROM")
[[ -n "${TO:-}" ]] && args+=(--to "$TO")
$PY producer/producer.py "${args[@]}"

cat > "$OUT/settings.json" <<JSON
{"cores": "$CORES", "repeats": $REPEATS, "days": "$DAYS", "events": "$EVENTS",
 "exchanges": "${EXCHANGES:-all}", "from": "${FROM:-}", "to": "${TO:-}",
 "host": "$(uname -sm)", "docker_cpus": "$(docker info --format '{{.NCPU}}')",
 "docker_mem_bytes": "$(docker info --format '{{.MemTotal}}')"}
JSON

echo "cores,repeat,job_id,state,run_ms" > "$OUT/runs.csv"
for c in $CORES; do
  taskmanager_with_cores "$c"
  for r in $(seq 1 "$REPEATS"); do
    reset_outputs
    log "run: $c core(s), repeat $r"
    sample_cpu "$OUT/cpu-c$c-r$r.csv" & sampler=$!
    id=$(submit_job "$c" --bounded --group "scal-c$c-r$r-$RANDOM" --subscriptions all ${JOB_ARGS:-})
    state=$(wait_finished "$id")
    kill "$sampler" 2>/dev/null || true
    save_job_details "$id" "$OUT/job-c$c-r$r.json"
    run_ms=$($PY -c 'import json,sys; j=json.load(open(sys.argv[1])); t=j["timestamps"]; print(t["FINISHED"]-t["RUNNING"] if "FINISHED" in t else -1)' "$OUT/job-c$c-r$r.json")
    echo "$c,$r,$id,$state,$run_ms" >> "$OUT/runs.csv"
    log "$state in $run_ms ms"
  done
done

if [[ "$VALIDATE" == "1" ]]; then
  log "checking the last run's output against the reference"
  $PY -m tools.dump_topic ema-results "$OUT/flink-output.jsonl" --idle 10
  # Quotes and housekeeping events never reach a window, so EVENTS=all has the same answer key as trades
  if [[ "$DAYS" == "weekdays" && -z "${EXCHANGES:-}${FROM:-}${TO:-}" ]]; then
    $PY -m tools.validate --flink "$OUT/flink-output.jsonl" --answer-key answer_key/answer-key-week.parquet \
      --report "$OUT/validation.json" || true
  else
    ref_args=(--days "$DAYS" -o "$OUT/reference.parquet")
    [[ -n "${EXCHANGES:-}" ]] && ref_args+=(--exchanges "$EXCHANGES")
    [[ -n "${FROM:-}" ]] && ref_args+=(--from "$FROM")
    [[ -n "${TO:-}" ]] && ref_args+=(--to "$TO")
    $PY -m tools.reference "${ref_args[@]}"
    $PY -m tools.validate --flink "$OUT/flink-output.jsonl" --reference "$OUT/reference.parquet" \
      --report "$OUT/validation.json" || true
  fi
fi

log "done: $OUT  (next: python -m experiments.analyze)"
