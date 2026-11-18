#!/usr/bin/env bash
# Latency under load: how long after a window becomes complete do its EMAs and advisory come out?
#
# A window is complete once the first event of the next window arrives (spec 3.1, item 7).
# The producer logs that moment for every window boundary; Flink stamps each result with the time
# it was produced (emitTs). Latency = emitTs - boundary time. It's split into
#   wait:       boundary -> window fired (watermark delay: out-of-order bound + slowest partition)
#   processing: window fired -> EMA result produced
#
# For every input rate the job runs live (unbounded) while the producer replays at that rate.
#
#   ./experiments/run_latency.sh
#
# Settings:
#   RATES="20000 50000 100000 0"   events per second (0 = as fast as the producer can go)
#   CORES=4                         TaskManager cores and job parallelism (try "1 2 4" too)
#   DAY=08-11-21  FROM=08:00  TO=12:00   the replayed slice (Monday's busy morning)
#   EVENTS=trades   LIMIT=3000000  (max events per run)   OOO_MS=5000 (out-of-order bound)
#   JOB_ARGS="--buffer-timeout-ms 5 --watermark-interval-ms 50"   extra job options (latency tuning)
source "$(dirname "$0")/common.sh"

RATES=${RATES:-"20000 50000 100000 0"}
CORES=${CORES:-4}
DAY=${DAY:-08-11-21}
FROM=${FROM:-08:00}
TO=${TO:-12:00}
EVENTS=${EVENTS:-trades}
LIMIT=${LIMIT:-3000000}
OOO_MS=${OOO_MS:-5000}
OUT=${OUT:-results/latency/$(date +%Y%m%d-%H%M%S)}
mkdir -p "$OUT"

max_cores=$(echo "$CORES" | tr ' ' '\n' | sort -n | tail -1)
stack_up "$max_cores"
cat > "$OUT/settings.json" <<JSON
{"rates": "$RATES", "cores": "$CORES", "day": "$DAY", "from": "$FROM", "to": "$TO", "events": "$EVENTS",
 "limit": $LIMIT, "ooo_ms": $OOO_MS, "job_args": "${JOB_ARGS:-}", "host": "$(uname -sm)", "docker_cpus": "$(docker info --format '{{.NCPU}}')"}
JSON

for c in $CORES; do
  taskmanager_with_cores "$c"
  for rate in $RATES; do
    tag="c$c-rate$rate"
    log "latency run: $c core(s), rate ${rate} events/s"
    reset_input
    reset_outputs
    id=$(submit_job "$c" --group "lat-$tag-$RANDOM" --subscriptions all --ooo-ms "$OOO_MS" ${JOB_ARGS:-})
    wait_running "$id"
    sample_cpu "$OUT/cpu-$tag.csv" & sampler=$!
    $PY producer/producer.py --days "$DAY" --from "$FROM" --to "$TO" --events "$EVENTS" \
      --rate "$rate" --limit "$LIMIT" --boundaries "$OUT/boundaries-$tag.csv" \
      --stats "$OUT/producer-$tag.json" --quiet
    # The end-of-stream marker flushes the last windows; give Flink a moment, then collect
    $PY -m tools.dump_topic ema-results "$OUT/results-$tag.jsonl" --idle 10
    kill "$sampler" 2>/dev/null || true
    save_job_details "$id" "$OUT/job-$tag.json"
    cancel_job "$id"
  done
done
log "done: $OUT  (next: python -m experiments.analyze)"
