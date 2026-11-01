# Payment producer, topic/s and event/s

Events: `PAYMENT_INITIATED`, `PAYMENT_AUTHORISED`, `PAYMENT_NOT_AUTHORISED`, `PAYMENT_VALIDATED`, `PAYMENT_INVALIDATED`, `PAYMENT_COMPLETED`, `PAYMENT_NOT_COMPLETED`.

The reasoning behind these values is in [payment_events_setup.md](payment_events_setup.md). The commands differ from the original template because some of its flags don't work as written. See "Issues Encountered" in that document.

## Topic configs
```bash
kubectl exec -it kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic payments.events.v1 \
  --partitions 100 \
  --replication-factor 3 \
  --config retention.ms=220924800000 \
  --config retention.bytes=-1 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=lz4 \
  --config max.message.bytes=1048576
```

### Consumer configs

The payment service only produces, so this console consumer is for inspecting the topic. It has its own group so it can't take partitions from a running service. These are the fraud service's settings, which are the strictest of the topic's consumers. `enable.auto.commit=false` makes `auto.commit.interval.ms` unused, and it's listed only because the template asks for it.

```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic payments.events.v1 \
  --property print.key=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --group payments-events-console \
  --consumer-property max.poll.records=50 \
  --consumer-property session.timeout.ms=10000 \
  --consumer-property heartbeat.interval.ms=3000 \
  --consumer-property auto.offset.reset=earliest \
  --consumer-property enable.auto.commit=false \
  --consumer-property auto.commit.interval.ms=5000 \
  --consumer-property max.poll.interval.ms=30000 \
  --consumer-property fetch.min.bytes=1 \
  --consumer-property fetch.max.wait.ms=10 \
  --consumer-property max.partition.fetch.bytes=262144 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

### Producer configs

The console producer always sends bytes, so the template's serializer properties are dropped. The real service uses `StringSerializer`.

```bash
kubectl exec -it kafka-0 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
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
```
