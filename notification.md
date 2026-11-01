# Notification producer, consumer, topic/s and event/s

The service consumes `PAYMENT_VALIDATED`, `PAYMENT_NOT_AUTHORISED`, `PAYMENT_INVALIDATED` and `PAYMENT_NOT_COMPLETED` from `payments.events.v1` in the group `notification-sender-group`, and produces `NOTIFICATION_PAYMENT_APPROVED` or `NOTIFICATION_PAYMENT_FAILED` to `notifications.events.v1`.

The reasoning behind these values is in [payment_events_setup.md](payment_events_setup.md). The commands differ from the original template because some of its flags don't work as written. See "Issues Encountered" in that document.

## Topic configs
```bash
kubectl exec -it kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic notifications.events.v1 \
  --partitions 30 \
  --replication-factor 3 \
  --config retention.ms=2592000000 \
  --config retention.bytes=-1 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=lz4 \
  --config max.message.bytes=1048576
```

### Consumer configs

These are the settings `notification-sender-group` uses on `payments.events.v1`. To inspect the topic, this console consumer reads `notifications.events.v1` in its own group, because reusing the service's group would take partitions from it. `enable.auto.commit=false` makes `auto.commit.interval.ms` unused, and it's listed only because the template asks for it.

```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic notifications.events.v1 \
  --property print.key=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --group notifications-console \
  --consumer-property max.poll.records=200 \
  --consumer-property session.timeout.ms=30000 \
  --consumer-property heartbeat.interval.ms=10000 \
  --consumer-property auto.offset.reset=earliest \
  --consumer-property enable.auto.commit=false \
  --consumer-property auto.commit.interval.ms=5000 \
  --consumer-property max.poll.interval.ms=120000 \
  --consumer-property fetch.min.bytes=1024 \
  --consumer-property fetch.max.wait.ms=100 \
  --consumer-property max.partition.fetch.bytes=1048576 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

### Producer configs

The console producer always sends bytes, so the template's serializer properties are dropped. The real service uses `StringSerializer`.

```bash
kubectl exec -it kafka-0 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic notifications.events.v1 \
  --property parse.key=true \
  --property key.separator=: \
  --producer-property acks=all \
  --producer-property retries=2147483647 \
  --producer-property max.in.flight.requests.per.connection=5 \
  --producer-property enable.idempotence=true \
  --producer-property compression.type=lz4 \
  --producer-property linger.ms=20 \
  --producer-property batch.size=65536 \
  --producer-property delivery.timeout.ms=60000 \
  --producer-property request.timeout.ms=10000
```
