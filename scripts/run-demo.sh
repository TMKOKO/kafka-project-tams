#!/usr/bin/env bash
# End-to-end run. Each section saves its real output to evidence/.
set -uo pipefail
source "$(dirname "$0")/lib.sh"

JOURNEYS=${JOURNEYS:-1000}
RATE=${RATE:-100}

echo "=== 01 Cluster"
{
  kubectl get pods -o wide
  echo
  broker kafka-metadata-quorum --bootstrap-server "$BOOTSTRAP" describe --status
  echo
  broker kafka-topics --version
} 2>&1 | tee "$EVIDENCE/01-cluster.txt"

echo "=== 02 Topics"
{
  for topic in payments.events.v1 fraud.scores.v1 notifications.events.v1 \
      fraud-scoring-group.dlq notification-sender-group.dlq audit-archiver-group.dlq; do
    echo "--- $topic"
    broker kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$topic"
    echo
  done
} 2>&1 | tee "$EVIDENCE/02-topics.txt" >/dev/null
grep -E '^Topic:.*PartitionCount' "$EVIDENCE/02-topics.txt"

echo "=== Warm-up: excludes first-request costs from the measurements"
payment_api -X POST "localhost:8080/simulate?count=100&seed=1&ratePerSec=$RATE" >/dev/null
wait_for_zero_lag 120
for app in payment-service fraud-service notification-service audit-service; do
  for pod in $(kubectl get pods -l app=$app -o name); do
    kubectl exec "$pod" -- curl -s -X POST localhost:8080/latency/reset
  done
done

echo "=== 03 Fixed scenarios"
payment_api -X POST localhost:8080/scenarios 2>&1 | tee "$EVIDENCE/03-scenarios.json"
echo

echo "=== 04 Simulation: $JOURNEYS journeys at $RATE/s"
payments_before=$(topic_total payments.events.v1)
payment_api -X POST "localhost:8080/simulate?count=$JOURNEYS&seed=42&ratePerSec=$RATE" 2>&1 | tee "$EVIDENCE/04-simulation.json"
echo
payments_after=$(topic_total payments.events.v1)
echo "Records added to payments.events.v1: $(( payments_after - payments_before ))" | tee -a "$EVIDENCE/04-simulation.json"

echo "=== 05 Consumer lag"
{
  echo "--- Taken immediately after the simulation returned"
  for group in fraud-scoring-group notification-sender-group audit-archiver-group; do
    echo "$group total lag: $(total_lag "$group")"
  done
  wait_for_zero_lag 180
  echo
  all_group_lag
  echo
  for group in fraud-scoring-group notification-sender-group audit-archiver-group; do
    broker kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --describe --group "$group" 2>/dev/null
  done
} 2>&1 | tee "$EVIDENCE/05-consumer-lag.txt" | grep -E 'total lag|zero lag|Lag did not|Stable|Empty|Rebalanc'

echo "=== 06 Latency against the SLAs"
{
  echo "Payment producer, target 150ms (event created to Kafka acknowledgement)"
  payment_api localhost:8080/latency; echo
  for app in fraud-service notification-service audit-service; do
    for pod in $(kubectl get pods -l app=$app -o name); do
      echo "$pod (event created to consumer processing)"
      kubectl exec "$pod" -- curl -s localhost:8080/latency; echo
    done
  done
} 2>&1 | tee "$EVIDENCE/06-latency.txt"

echo "=== 07 Ordering"
{
  echo "--- Audit verification across every archived payment"
  audit_api localhost:8080/audit/verify; echo
  id=$(grep -oE '"paymentId":"[^"]+"' "$EVIDENCE/03-scenarios.json" | head -1 | cut -d'"' -f4)
  partition=$(grep -oE '"partition":[0-9]+' "$EVIDENCE/03-scenarios.json" | head -1 | cut -d: -f2)
  echo
  echo "--- Timeline of payment $id"
  audit_api "localhost:8080/audit/payments/$id"; echo
  echo
  echo "--- The same payment read directly from partition $partition"
  broker kafka-console-consumer --bootstrap-server "$BOOTSTRAP" --topic payments.events.v1 \
    --partition "$partition" --offset earliest --timeout-ms 8000 \
    --property print.key=true --property print.partition=true --property print.offset=true \
    --property print.headers=true 2>/dev/null | grep "$id"
} 2>&1 | tee "$EVIDENCE/07-ordering.txt"

echo "=== 08 Service output samples"
{
  for app in fraud-service notification-service audit-service; do
    echo "--- $app"
    kubectl logs -l app=$app --tail=8 --prefix 2>&1 | grep -E 'SCORED|NOTIFIED|GATEWAY|ARCHIVED' | cut -c1-260
  done
  echo "--- payment-service"
  kubectl logs deploy/payment-service --tail=2000 2>&1 | grep -E 'SENT' | head -8 | cut -c1-220
  echo
  echo "--- Fraud levels and notification records"
  echo "notifications.events.v1 total records: $(topic_total notifications.events.v1)"
  echo "fraud.scores.v1 total records: $(topic_total fraud.scores.v1)"
  echo "payments.events.v1 total records: $(topic_total payments.events.v1)"
  echo
  echo "--- Sample of fraud.scores.v1 using a console consumer in its own group"
  broker kafka-console-consumer --bootstrap-server "$BOOTSTRAP" --topic fraud.scores.v1 \
    --group fraud-scores-console --from-beginning --max-messages 2 --timeout-ms 15000 \
    --property print.key=true --property print.partition=true --property print.offset=true 2>/dev/null
  echo "--- Sample of notifications.events.v1 using a console consumer in its own group"
  broker kafka-console-consumer --bootstrap-server "$BOOTSTRAP" --topic notifications.events.v1 \
    --group notifications-console --from-beginning --max-messages 2 --timeout-ms 15000 \
    --property print.key=true --property print.partition=true --property print.offset=true 2>/dev/null
} 2>&1 | tee "$EVIDENCE/08-service-output.txt" >/dev/null
tail -12 "$EVIDENCE/08-service-output.txt" | cut -c1-240
