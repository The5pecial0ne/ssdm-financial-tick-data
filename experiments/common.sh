#!/usr/bin/env bash
# Helpers shared by the experiment scripts. Source it, don't run it.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"
REST=${REST:-http://localhost:8081}
PY=${PY:-python3}
KT="docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092"

log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

die() { printf '\n[%s] FAILED: %s\n' "$(date +%H:%M:%S)" "$*" >&2; exit 1; }

need() {
  command -v "$1" >/dev/null || die "missing tool: $1"
}
need docker; need curl; need "$PY"
$PY -c 'import confluent_kafka, pandas' 2>/dev/null \
  || die "$PY can't import the project's packages; run: pip install -r requirements.txt"

# How long to wait for the stack to come up and for one job to finish before giving up
START_TIMEOUT=${START_TIMEOUT:-600}
JOB_TIMEOUT=${JOB_TIMEOUT:-3600}

# Runs a command every 2 s until it succeeds; gives up after $1 seconds
retry_for() {
  local secs=$1 what=$2; shift 2
  local end=$((SECONDS + secs))
  until "$@"; do
    (( SECONDS < end )) || die "gave up after ${secs}s waiting for $what"
    sleep 2
  done
}
rest_up() { curl -sf "$REST/overview" >/dev/null; }
kafka_up() { $KT --list >/dev/null 2>&1; }

stack_up() {
  log "starting Kafka and Flink (first run builds the images, a few minutes)"
  TM_CPUS=${1:-4} TM_SLOTS=${1:-4} docker compose up -d --build kafka kafka-init jobmanager taskmanager
  retry_for "$START_TIMEOUT" "the Flink REST API at $REST" rest_up
  retry_for "$START_TIMEOUT" "Kafka" kafka_up
}

# Recreates one topic, so a run never reads what an earlier run left behind.
# retention.ms=-1: Flink stamps its output records with their event time (2021), so with Kafka's
# default 7-day retention the broker deletes them at its next 5-minute retention check, mid-run.
recreate_topic() {
  local topic=$1 parts=${2:-1}
  $KT --delete --if-exists --topic "$topic" >/dev/null 2>&1 || true
  while $KT --list | grep -qx "$topic"; do sleep 1; done
  $KT --create --topic "$topic" --partitions "$parts" --replication-factor 1 --config retention.ms=-1 >/dev/null
}

reset_outputs() {
  for t in ema-results advisories late-trades; do recreate_topic "$t" 1; done
}

reset_input() {
  recreate_topic ticks "${PARTITIONS:-8}"
}

# Restarts the TaskManager with N CPU cores and N slots, then waits until Flink sees exactly N slots
taskmanager_with_cores() {
  local n=$1
  log "TaskManager: $n core(s), $n slot(s)"
  TM_CPUS=$n TM_SLOTS=$n docker compose up -d --no-deps --force-recreate taskmanager >/dev/null
  retry_for "$START_TIMEOUT" "a TaskManager with $n slot(s)" slots_are "$n 1"
}
slots_are() {
  [[ "$(curl -sf "$REST/overview" | $PY -c 'import json,sys; d=json.load(sys.stdin); print(d["slots-total"], d["taskmanagers"])' 2>/dev/null)" == "$1" ]]
}

# Submits the job detached; prints the job id
submit_job() {
  local par=$1 out id; shift
  out=$(docker compose exec -T jobmanager flink run -d -p "$par" /opt/ssdm/trading-job.jar "$@" 2>&1) || true
  id=$(sed -n 's/.*JobID \([0-9a-f]\{32\}\).*/\1/p' <<<"$out" | tail -1)
  [[ -n "$id" ]] || { echo "$out" >&2; die "could not submit the job (parallelism $par)"; }
  echo "$id"
}

job_state() { curl -sf "$REST/jobs/$1" | $PY -c 'import json,sys; print(json.load(sys.stdin)["state"])'; }

wait_running() {
  until [[ "$(job_state "$1")" == "RUNNING" ]]; do sleep 1; done
  # ...and until every task is actually running, not just scheduled
  until curl -sf "$REST/jobs/$1" | $PY -c 'import json,sys; v=json.load(sys.stdin)["vertices"]; sys.exit(0 if all(x["status"]=="RUNNING" for x in v) else 1)'; do sleep 1; done
}

# Prints the job's final state; cancels it and prints TIMEOUT if it runs longer than JOB_TIMEOUT
wait_finished() {
  local s end=$((SECONDS + JOB_TIMEOUT))
  while true; do
    s=$(job_state "$1" 2>/dev/null || echo UNKNOWN)
    case $s in FINISHED|FAILED|CANCELED) echo "$s"; return;; esac
    if (( SECONDS >= end )); then cancel_job "$1"; echo TIMEOUT; return; fi
    sleep 1
  done
}

cancel_job() { curl -sf -X PATCH "$REST/jobs/$1?mode=cancel" >/dev/null || true; }

# Saves the job's REST details (timings, per-vertex and per-subtask record counts) as JSON
save_job_details() {
  local id=$1 out=$2
  $PY - "$REST" "$id" "$out" <<'PYEOF'
import json, sys, urllib.request
rest, jid, out = sys.argv[1:]
get = lambda p: json.load(urllib.request.urlopen(rest + p))
job = get(f"/jobs/{jid}")
for v in job["vertices"]:
    v["subtasks"] = get(f"/jobs/{jid}/vertices/{v['id']}").get("subtasks", [])
json.dump(job, open(out, "w"), indent=1)
PYEOF
}

# Samples CPU% and memory of the containers once a second until killed
sample_cpu() {
  local out=$1
  echo "wall_ms,container,cpu_percent,mem" > "$out"
  while true; do
    local now; now=$($PY -c 'import time; print(int(time.time()*1000))')
    docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' kafka jobmanager taskmanager 2>/dev/null \
      | sed "s/^/$now,/; s/%//" >> "$out" || true
  done
}
