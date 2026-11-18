#!/usr/bin/env bash
# Packs everything the system runs on (our two images plus Kafka) into one file, so another machine
# can run it without building anything or reaching Docker Hub. Copy the file over with the repository.
#
#   ./scripts/export-images.sh                         # for machines with this machine's CPU type
#   PLATFORM=linux/amd64 ./scripts/export-images.sh    # for an ordinary Windows or Linux PC, when
#                                                      # you are on an Apple Silicon Mac
#   NO_BUILD=1 ./scripts/export-images.sh              # save the images you already built
#
# On the other machine, from the repository folder (docs/TESTING.md has the details):
#   docker load -i ssdm-images.tar.gz
#   docker compose --profile demo up -d --no-build
#
# The file is about 1 GB. Settings: OUT=<file> (default ssdm-images.tar.gz)
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=${OUT:-ssdm-images.tar.gz}
images=(ssdm-flink:1.0 ssdm-python:1.0 apache/kafka:3.9.0)

if [[ -n "${PLATFORM:-}" ]]; then
  # Building for another CPU type replaces this machine's own images under the same names, so put
  # the native ones back at the end. Otherwise the experiments would later run emulated, and slowly.
  trap 'echo "putting back the images for this machine ..."; unset DOCKER_DEFAULT_PLATFORM
        docker compose --profile demo build -q && docker pull -q apache/kafka:3.9.0 >/dev/null' EXIT
  export DOCKER_DEFAULT_PLATFORM=$PLATFORM
fi

if [[ -z "${NO_BUILD:-}" || -n "${PLATFORM:-}" ]]; then
  echo "building the images ${PLATFORM:+for $PLATFORM }..."
  docker compose --profile demo build
  docker pull ${PLATFORM:+--platform "$PLATFORM"} apache/kafka:3.9.0
fi

echo "saving ${images[*]} to $OUT ..."
docker save "${images[@]}" | gzip -1 > "$OUT"
echo "done: $OUT ($(du -h "$OUT" | cut -f1))"
