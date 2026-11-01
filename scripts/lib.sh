#!/usr/bin/env bash
# Helpers shared by the demo and failure-test scripts.
export MSYS_NO_PATHCONV=1
BOOTSTRAP=kafka-service:9092
EVIDENCE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/evidence"
mkdir -p "$EVIDENCE"

# Runs a Kafka CLI tool inside a broker. Uses a live broker, because kafka-0 may be down in failure tests.
broker() {
  local pod
  pod=$(kubectl get pods -l app=kafka --field-selector=status.phase=Running \
    -o jsonpath='{range .items[?(@.status.containerStatuses[0].ready==true)]}{.metadata.name}{"\n"}{end}' | head -1)
  kubectl exec -i "$pod" -- "$@"
}

payment_api() {
  kubectl exec deploy/payment-service -- curl -s -m 400 "$@"
}

audit_api() {
  kubectl exec deploy/audit-service -- curl -s -m 30 "$@"
}

# Sum of the LAG column for a group. Prints "unknown" if the group can't be described.
total_lag() {
  broker kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --describe --group "$1" 2>/dev/null |
    awk 'BEGIN { sum = 0; seen = 0 } $1 == group && $6 ~ /^[0-9]+$/ { sum += $6; seen = 1 } END { if (seen) print sum; else print "unknown" }' group="$1"
}

# Waits until all three groups report zero lag, printing the time it took.
wait_for_zero_lag() {
  local limit=${1:-180} start=$SECONDS group lag caught_up
  while (( SECONDS - start < limit )); do
    caught_up=1
    for group in fraud-scoring-group notification-sender-group audit-archiver-group; do
      lag=$(total_lag "$group")
      [ "$lag" = "0" ] || caught_up=0
    done
    if (( caught_up )); then
      echo "All groups at zero lag after $(( SECONDS - start ))s"
      return 0
    fi
    sleep 2
  done
  echo "Lag did not reach zero within ${limit}s"
  for group in fraud-scoring-group notification-sender-group audit-archiver-group; do
    echo "  $group lag=$(total_lag "$group")"
  done
  return 1
}

# Sum of the end offsets for a topic, which is the number of records it has ever received.
topic_total() {
  broker kafka-get-offsets --bootstrap-server "$BOOTSTRAP" --topic "$1" 2>/dev/null |
    awk -F: '{ sum += $3 } END { print sum + 0 }'
}

# Pulls a numeric top-level field out of a JSON string.
json_number() {
  grep -oE "\"$2\":[0-9]+" <<<"$1" | head -1 | cut -d: -f2
}

all_group_lag() {
  for group in fraud-scoring-group notification-sender-group audit-archiver-group; do
    broker kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --describe --group "$group" --state 2>/dev/null
    echo "$group total lag: $(total_lag "$group")"
  done
}
