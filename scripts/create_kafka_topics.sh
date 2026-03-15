#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${1:-localhost:9092}"

for topic in comments.raw comments.cleaned comments.ml_tasks comments.ml_results comments.dlq; do
  docker exec comments-kafka kafka-topics \
    --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions 12 \
    --replication-factor 1
done

echo "Topics ready"
