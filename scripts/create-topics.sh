#!/usr/bin/env bash
# Creates every topic with its designed configuration. Safe to re-run: existing topics are left as they are.
set -euo pipefail
export MSYS_NO_PATHCONV=1

SEVEN_YEARS_MS=220924800000   # 2557 days: 7 x 365 + 2 leap days, so retention never falls short of 7 years
THIRTY_DAYS_MS=2592000000

create_topic() {
  local topic=$1 partitions=$2 retention=$3
  kubectl exec kafka-0 -- kafka-topics \
    --bootstrap-server kafka-service:9092 \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 3 \
    --config retention.ms="$retention" \
    --config retention.bytes=-1 \
    --config cleanup.policy=delete \
    --config min.insync.replicas=2 \
    --config compression.type=lz4 \
    --config max.message.bytes=1048576
}

create_topic payments.events.v1 100 "$SEVEN_YEARS_MS"
create_topic fraud.scores.v1 30 "$SEVEN_YEARS_MS"
create_topic notifications.events.v1 30 "$THIRTY_DAYS_MS"

# DLQs can hold regulated events, so they keep the 7-year retention. Low volume needs few partitions.
create_topic fraud-scoring-group.dlq 3 "$SEVEN_YEARS_MS"
create_topic notification-sender-group.dlq 3 "$SEVEN_YEARS_MS"
create_topic audit-archiver-group.dlq 3 "$SEVEN_YEARS_MS"
