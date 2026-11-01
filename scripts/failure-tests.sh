#!/usr/bin/env bash
# Breaks things on purpose and records what happens. Saves its real output to evidence/.
set -uo pipefail
source "$(dirname "$0")/lib.sh"

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

simulate_in_background() {
  payment_api -X POST "localhost:8080/simulate?count=$1&seed=$2&ratePerSec=$3" >"$TMP/sim.json" 2>&1 &
  SIM_PID=$!
}

wait_for_brokers() {
  kubectl rollout status statefulset/kafka --timeout=300s 2>&1 | tail -1
  kubectl wait --for=condition=ready pod -l app=kafka --timeout=300s 2>&1 | tail -3
}

wait_for_full_isr() {
  local start=$SECONDS count
  while (( SECONDS - start < 180 )); do
    count=$(broker kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --under-replicated-partitions 2>/dev/null | grep -c 'Topic:')
    if [ "$count" = "0" ]; then
      echo "No under-replicated partitions after $(( SECONDS - start ))s"
      return 0
    fi
    sleep 3
  done
  echo "Still $count under-replicated partitions after 180s"
}

echo "=== 09 One broker down under load"
{
  before=$(topic_total payments.events.v1)
  simulate_in_background 600 101 20
  sleep 8
  echo "--- Deleting kafka-2 while 600 journeys run at 20/s"
  kubectl delete pod kafka-2 --wait=false
  sleep 6
  echo "--- Under-replicated partitions while it is down (first 4 lines, then count)"
  broker kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --under-replicated-partitions --topic payments.events.v1 2>/dev/null >"$TMP/urp.txt"
  head -4 "$TMP/urp.txt"
  echo "payments.events.v1 under-replicated partitions: $(grep -c 'Topic:' "$TMP/urp.txt")"
  wait "$SIM_PID"
  echo "--- Simulation result"
  cat "$TMP/sim.json"; echo
  acknowledged=$(json_number "$(cat "$TMP/sim.json")" eventsAcknowledged)
  wait_for_brokers
  wait_for_full_isr
  after=$(topic_total payments.events.v1)
  echo "Events acknowledged: $acknowledged. Records added to the topic: $(( after - before ))"
  wait_for_zero_lag 180
  audit_api localhost:8080/audit/verify; echo
} 2>&1 | tee "$EVIDENCE/09-one-broker-down.txt"

echo "=== 10 Two brokers down"
{
  before=$(topic_total payments.events.v1)
  echo "--- Scaling to 1 broker, which leaves one of three replicas and one of three controller voters"
  kubectl scale statefulset/kafka --replicas=1
  kubectl wait --for=delete pod/kafka-1 pod/kafka-2 --timeout=120s 2>&1 | tail -2
  echo "--- One payment request while only kafka-0 is running"
  body='{"customerId":"cust-outage","accountId":"acc-1","merchantId":"merch-1","amountMinor":1000,"currency":"ZAR","channel":"WEB","paymentType":"CARD","deviceId":"device-1","ipCountry":"ZA","outcome":"COMPLETED"}'
  kubectl exec deploy/payment-service -- curl -s -m 60 -w '\nHTTP %{http_code} after %{time_total}s\n' \
    -X POST localhost:8080/payments -H 'Content-Type: application/json' -d "$body"
  echo "--- What the producer logged"
  kubectl logs deploy/payment-service --since=70s 2>&1 | grep -E 'UNSENT' | cut -c1-320 | tail -3
  echo "--- Consumer behaviour during the outage (counts of warnings in the last 70s)"
  kubectl logs -l app=fraud-service --since=70s --tail=-1 2>&1 | grep -cE ' (WARN|ERROR) ' | sed 's/^/fraud warnings: /'
  echo "--- Restoring brokers"
  kubectl scale statefulset/kafka --replicas=3
  wait_for_brokers
  wait_for_full_isr
  echo "--- A new payment after recovery"
  kubectl exec deploy/payment-service -- curl -s -m 60 -w '\nHTTP %{http_code} after %{time_total}s\n' \
    -X POST localhost:8080/payments -H 'Content-Type: application/json' -d "$body"
  after=$(topic_total payments.events.v1)
  echo "Records added across the outage and recovery: $(( after - before ))"
  wait_for_zero_lag 240
  audit_api localhost:8080/audit/verify; echo
} 2>&1 | tee "$EVIDENCE/10-two-brokers-down.txt"

echo "=== 11 Poison message"
{
  echo "--- Sending a malformed record using the console producer with the payment settings"
  echo 'poison-1:this is not json' | broker kafka-console-producer \
    --bootstrap-server "$BOOTSTRAP" \
    --topic payments.events.v1 \
    --property parse.key=true \
    --property key.separator=: \
    --producer-property acks=all \
    --producer-property retries=2147483647 \
    --producer-property max.in.flight.requests.per.connection=5 \
    --producer-property enable.idempotence=true \
    --producer-property compression.type=lz4 \
    --producer-property linger.ms=5 \
    --producer-property batch.size=65536 \
    --producer-property delivery.timeout.ms=30000 \
    --producer-property request.timeout.ms=5000
  echo "--- Sending valid journeys straight after it"
  payment_api -X POST localhost:8080/scenarios | grep -oE '"outcome":"[A-Z_]+"' | tr '\n' ' '; echo
  wait_for_zero_lag 120
  for dlq in fraud-scoring-group.dlq notification-sender-group.dlq audit-archiver-group.dlq; do
    echo "--- $dlq"
    broker kafka-console-consumer --bootstrap-server "$BOOTSTRAP" --topic "$dlq" --from-beginning \
      --timeout-ms 10000 --property print.key=true --property print.headers=true 2>/dev/null | grep poison-1 | cut -c1-420
  done
  audit_api localhost:8080/audit/verify; echo
} 2>&1 | tee "$EVIDENCE/11-poison-message.txt"

echo "=== 12 Fraud pod killed under load"
{
  simulate_in_background 600 102 20
  sleep 8
  victim=$(kubectl get pods -l app=fraud-service -o name | head -1)
  echo "--- Deleting $victim while 600 journeys run at 20/s"
  kubectl delete "$victim" --wait=false
  sleep 6
  broker kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --describe --group fraud-scoring-group --state 2>/dev/null
  wait "$SIM_PID"
  cat "$TMP/sim.json"; echo
  kubectl rollout status deploy/fraud-service --timeout=240s 2>&1 | tail -1
  wait_for_zero_lag 240
  broker kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --describe --group fraud-scoring-group --state 2>/dev/null
  echo "--- Scores against payments, across everything run so far"
  broker kafka-console-consumer --bootstrap-server "$BOOTSTRAP" --topic fraud.scores.v1 --from-beginning \
    --timeout-ms 25000 2>/dev/null | grep -oE '"eventId":"[^"]+"' >"$TMP/ids.txt"
  echo "fraud.scores.v1 records: $(wc -l <"$TMP/ids.txt"), unique eventIds: $(sort -u "$TMP/ids.txt" | wc -l)"
  audit_api localhost:8080/audit/verify; echo
} 2>&1 | tee "$EVIDENCE/12-fraud-pod-killed.txt"
