#!/usr/bin/env bash
# Runs the Flink job on your own machine, without Docker or Kafka, straight from CSV files.
# Flink starts a small in-process cluster, so this is the quickest way to check correctness.
#
#   ./flink-job/run-local.sh <csv>[,<csv>...] <output.jsonl> [extra job options]
#
# Example (from the repo root):
#   ./flink-job/run-local.sh analysis/test-slice.csv results/test-slice-flink.jsonl
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
input="${1:?give one or more CSV files, comma-separated}"
output="${2:?give an output .jsonl path}"
shift 2

cd "$here"
if [[ ! -f target/flink-job-1.0.jar ]]; then
  mvn -q -B package -DskipTests
fi
if [[ ! -f target/classpath.txt || pom.xml -nt target/classpath.txt ]]; then
  mvn -q -B dependency:build-classpath -Dmdep.outputFile=target/classpath.txt -Dmdep.includeScope=compile
fi
cd - >/dev/null

mkdir -p "$(dirname "$output")"
# Absolute paths, because Flink's file source resolves relative ones on its own terms
abs_inputs=$(python3 -c 'import os,sys; print(",".join(os.path.abspath(x) for x in sys.argv[1].split(",")))' "$input")

java ${JAVA_OPTS:--Xmx2g} \
  --add-opens java.base/java.util=ALL-UNNAMED \
  --add-opens java.base/java.lang=ALL-UNNAMED \
  -cp "$here/target/flink-job-1.0.jar:$(cat "$here/target/classpath.txt")" \
  ssdm.TradingJob --input-file "$abs_inputs" --output-file "$output" "$@"
