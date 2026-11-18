#!/usr/bin/env bash
# Runs the Flink job on your own machine inside a small in-process Flink cluster (no Docker).
#
# File mode, straight from CSV files, the quickest way to check correctness:
#   ./flink-job/run-local.sh <csv>[,<csv>...] <output.jsonl> [extra job options]
#   ./flink-job/run-local.sh analysis/test-slice.csv results/test-slice-flink.jsonl
#
# Any other job options (for example against a Kafka broker on localhost):
#   ./flink-job/run-local.sh -- --bootstrap localhost:9092 --bounded --parallelism 4
#
# Works on Linux and macOS, and on Windows from Git Bash or WSL.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
mode=file
if [[ "${1:-}" == "--" ]]; then
  mode=args
  shift
else
  input="${1:?give one or more CSV files, comma-separated}"
  output="${2:?give an output .jsonl path}"
  shift 2
fi

cd "$here"
if [[ ! -f target/flink-job-1.0.jar ]]; then
  mvn -q -B package -DskipTests
fi
if [[ ! -f target/classpath.txt || pom.xml -nt target/classpath.txt ]]; then
  mvn -q -B dependency:build-classpath -Dmdep.outputFile=target/classpath.txt -Dmdep.includeScope=compile
fi
cd - >/dev/null

# Java on Windows wants C:\... paths joined with ';' (Maven already wrote classpath.txt that way)
jar="$here/target/flink-job-1.0.jar" sep=":"
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) jar="$(cygpath -w "$jar")" sep=";" ;;
esac
py=$(command -v python3 || command -v python)

run() {
  java ${JAVA_OPTS:--Xmx2g} \
    --add-opens java.base/java.util=ALL-UNNAMED \
    --add-opens java.base/java.lang=ALL-UNNAMED \
    -cp "$jar$sep$(cat "$here/target/classpath.txt")" \
    ssdm.TradingJob "$@"
}

if [[ $mode == args ]]; then
  run "$@"
else
  mkdir -p "$(dirname "$output")"
  # Absolute paths, because Flink's file source resolves relative ones on its own terms
  abs_inputs=$("$py" -c 'import os,sys; print(",".join(os.path.abspath(x) for x in sys.argv[1].split(",")))' "$input")
  run --input-file "$abs_inputs" --output-file "$output" "$@"
fi
