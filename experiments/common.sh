#!/usr/bin/env bash
# Helpers shared by the experiment scripts. Source it, don't run it.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"
REST=${REST:-http://localhost:8081}
PY=${PY:-python3}
KT="docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092"

log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

need() {
  command -v "$1" >/dev/null || { echo "missing tool: $1"; exit 1; }
}
need docker; need curl; need "$PY"

stack_up() {
  log "starting Kafka and Flink (first run builds the images, a few minutes)"
  TM_CPUS=${1:-4} TM_SLOTS=${1:-4} docker compose up -d --build kafka kafka-init jobmanager taskmanager
  until curl -sf "$REST/overview" >/dev/null; do sleep 2; done
  until $KT --list >/dev/null 2>&1; do sleep 2; done
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
  until [[ "$(curl -sf "$REST/overview" | $PY -c 'import json,sys; d=json.load(sys.stdin); print(d["slots-total"], d["taskmanagers"])' 2>/dev/null)" == "$n 1" ]]; do
    sleep 2
  done
}

# Submits the job detached; prints the job id
submit_job() {
  local par=$1; shift
  docker compose exec -T jobmanager flink run -d -p "$par" /opt/ssdm/trading-job.jar "$@" \
    | sed -n 's/.*JobID \([0-9a-f]\{32\}\).*/\1/p' | tail -1
}

job_state() { curl -sf "$REST/jobs/$1" | $PY -c 'import json,sys; print(json.load(sys.stdin)["state"])'; }

wait_running() {
  until [[ "$(job_state "$1")" == "RUNNING" ]]; do sleep 1; done
  # ...and until every task is actually running, not just scheduled
  until curl -sf "$REST/jobs/$1" | $PY -c 'import json,sys; v=json.load(sys.stdin)["vertices"]; sys.exit(0 if all(x["status"]=="RUNNING" for x in v) else 1)'; do sleep 1; done
}

wait_finished() {
  local s
  while true; do
    s=$(job_state "$1")
    case $s in FINISHED|FAILED|CANCELED) echo "$s"; return;; esac
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
