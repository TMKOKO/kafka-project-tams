# Digital Payments Event Streaming: Design and Setup

This POC replaces synchronous payment processing with events on a 3-broker Kafka cluster. A payment service publishes each state change, and fraud, notification and audit services consume those events independently. All outputs below are real, with long lines and repeated rows trimmed for readability, and the raw files are in [`evidence/`](evidence/).

## Results at a glance

| Requirement | Result |
|---|---|
| Ordering per payment | **Met.** 7,079 payments and 26,600 events checked, with 0 violations |
| No acknowledged events lost | **Met.** Every record reconciles to the archive, including through broker failures |
| One broker can fail | **Met.** 2,275 of 2,275 events acknowledged while a broker was down |
| At least 3 consumer groups, with lag near zero | **Met.** 3 groups, all at lag 0 |
| Notification lag under 2s | **Met at p99** in both quiet runs, with a worst case of 3.1s |
| Payment lag under 150ms | **Partly met.** p50 of 13ms at 20 journeys/s, but p95 of 157ms and p99 of 1.25s |
| Fraud lag under 50ms | **Met at p50 only.** About 14ms at p50, but up to 868ms at p95 |
| 1000 Mb/s peak | **Not tested.** The partition counts are sized for it, but the highest full-system load run was about 1.3 Mb/s on a laptop |

The latency misses are reported as measured. Section 7 explains what caused the largest ones, and which causes are confirmed and which are not.

## Assumptions about the brief

| The brief says | Interpretation |
|---|---|
| At least 3 consumer groups, but specifies only fraud and notifications | Added an audit consumer, which serves the 7-year compliance requirement |
| Notification consumer reads "payment failed" events | No such event is defined, so failed means `PAYMENT_NOT_AUTHORISED`, `PAYMENT_INVALIDATED` or `PAYMENT_NOT_COMPLETED` |
| Notifications are "fraud payment approved or failed" | Read as the outcome of the payment: `VALIDATED` gives approved, and any failure gives failed |
| Fraud consumer runs "realtime fraud scoring" | Scores are published but do not block authorisation, because the brief's flow has no fraud step and aims to decouple the services. Gating is the obvious production next step |
| Throughput is given in "Mb/s" | Taken as megabits. Partition counts use ratios, so they would be the same if it meant megabytes |
| Cluster uses Zookeeper | Used KRaft instead, because Zookeeper is removed in Kafka 4 and KRaft needs fewer pods |

---

## 1. Design Decisions

### Architecture

```
payment-service ──> payments.events.v1 ──┬─> fraud-service ────────> fraud.scores.v1 ──┐
  (REST + simulator)                     ├─> notification-service ─> notifications.events.v1
                                         └─> audit-service <───────────────────────────┘
```

The services are Java 21 and Spring Boot, using the official Kafka client so every setting below maps directly to a client property. They run on Docker Desktop Kubernetes with a 3-broker Confluent Kafka 7.7.1 cluster.

### 1.1 Topic design

| Setting | `payments.events.v1` | `fraud.scores.v1` | `notifications.events.v1` |
|---|---|---|---|
| Partitions | 100 | 30 | 30 |
| Replication factor | 3 | 3 | 3 |
| `min.insync.replicas` | 2 | 2 | 2 |
| `retention.ms` | 220924800000 (2557 days) | 220924800000 (2557 days) | 2592000000 (30 days) |
| `retention.bytes` | -1 | -1 | -1 |
| `cleanup.policy` | delete | delete | delete |
| `compression.type` | lz4 | lz4 | lz4 |
| `max.message.bytes` | 1048576 | 1048576 | 1048576 |

**Name.** The pattern is `<domain>.<content>.v<major>`. The version makes breaking schema changes possible through a new topic. Dead-letter queues (DLQs) are named `<consumer-group>.dlq`, so each one shows who failed to process the record.

**Partitions.** The formula is `max(throughput ÷ producer rate, throughput ÷ consumer rate)`.

- **Payments:** `max(1000÷100, 1000÷10)` = **100**. Consumers are the limit, at one instance per partition.
- **Fraud and notifications:** a full journey has 4 events, and about one of them produces a score or a notification. That gives 250 Mb/s ÷ 10 = 25 partitions, plus 20% headroom = **30**. This assumes similar event sizes. Headroom matters because adding partitions later changes which partition a key maps to, which breaks per-key ordering across the change.
- **Should payment types get their own partitions or topics?** No. Key hashing spreads them evenly, and separate topics would lose ordering across a payment's states.
- **A cost of one shared topic:** filtering happens after fetching, so the fraud and notification groups still fetch all 1000 Mb/s and need up to 100 instances each at peak. Splitting by event type would cut that, but ordering per payment is the harder requirement, so one topic wins.

**Replication factor 3 and `min.insync.replicas` 2.** Any one broker can fail with no data loss and no write outage. With RF 2 and min ISR 2, one failure would stop writes. With RF 2 and min ISR 1, one failure could lose acknowledged data. RF 3 costs three times the storage, which is justified for payment records. Each acknowledged write waits for at least one follower, and that latency is accepted in exchange for durability.

**Retention.**

- **Why 2557 days:** that's 7×365 plus 2 leap days, so retention can never fall short of 7 years. The value 2557 × 86,400,000 gives 220,924,800,000 ms.
- **Why no size limit:** `retention.bytes=-1`, because a size cap could delete regulated data early. Time-only retention means storage has to be monitored instead.
- **Why notifications keep only 30 days:** the 7-year requirement names fraud and payment events only, and notifications can be regenerated from them.
- **Storage reality:** at a sustained 1000 Mb/s (125 MB/s), 7 years is about 27.6 PB per copy, or about 83 PB at RF 3, before compression. That's an upper bound, since peak isn't sustained, but it shows that production would need tiered storage or archiving rather than broker disks.

**Cleanup policy: delete.** Payment events are immutable facts and the topic is an event stream, not a state store. Log compaction keeps only the latest value per key, which would destroy the `INITIATED → AUTHORISED → ...` history the audit needs.

**Compression: lz4.** JSON events repeat the same field names, so they compress well, and lz4 has a low CPU cost, which suits the latency targets. gzip compresses more but costs more CPU, and zstd is a reasonable alternative. The compression ratio was not measured in this POC.

**`max.message.bytes`: 1 MiB.** Average events are small: 460 bytes for payments and 356 for fraud. This limit applies to whole compressed batches, so it has to stay above `batch.size` (64 KiB), even though single events are far smaller.

**Message schema (`PaymentEvent`).**

| Field | Type | Purpose |
|---|---|---|
| `eventId` | UUID string | Unique event ID, used for deduplication and audit |
| `schemaVersion` | int | Allows the schema to evolve |
| `eventType` | enum | The state this event records |
| `paymentId` | UUID string | Partition key, links a payment's events |
| `customerId`, `accountId`, `merchantId` | string | Parties, used for notifications and reconciliation |
| `amountMinor` | long | Amount in minor units (cents). Never a float, to avoid rounding errors |
| `currency` | ISO 4217 string | Currency of the amount |
| `channel`, `paymentType` | string | Fraud signals and reporting |
| `deviceId`, `ipCountry` | string | Fraud signals. A null device counts as unrecognised |
| `reasonCode` | string or null | Set only on failure events |
| `occurredAt` | ISO 8601 timestamp | When it happened, also used to measure lag |
| `correlationId` | UUID string | Links everything from one journey across services |

Fraud and notification events carry the same identifiers plus `causationId`, which holds the source event's ID. All records also have an `event-type` header, so consumers can skip records without parsing JSON.

### 1.2 Producer design

**Event types.** Payments follow the flow in the brief:

```
PAYMENT_INITIATED ─┬─> PAYMENT_NOT_AUTHORISED
                   └─> PAYMENT_AUTHORISED ─┬─> PAYMENT_INVALIDATED
                                           └─> PAYMENT_VALIDATED ─┬─> PAYMENT_NOT_COMPLETED
                                                                  └─> PAYMENT_COMPLETED
```

The fraud producer publishes `FRAUD_SCORE_LOW`, `MEDIUM` or `HIGH`. The notification producer publishes `NOTIFICATION_PAYMENT_APPROVED` or `NOTIFICATION_PAYMENT_FAILED`.

**Partition key: `paymentId`, on all topics.** Kafka only guarantees order within a partition, so a payment's events must share a key. `customerId` would also keep them in order and would help velocity-based fraud rules. But the sizing allows exactly 10 Mb/s per partition at peak, so there's no slack for a hot key such as a bulk corporate account. A random UUID spreads evenly. The cost is that customer-level fraud rules would need an external store.

**Producer settings.**

| Setting | Payment | Fraud | Notification | Why |
|---|---|---|---|---|
| `acks` | all | all | all | A lost payment event costs far more than a few milliseconds |
| `enable.idempotence` | true | true | true | Broker discards duplicates caused by producer retries |
| `retries` | 2147483647 | 2147483647 | 2147483647 | Time limits below bound the retrying, not a count |
| `max.in.flight.requests.per.connection` | 5 | 5 | 5 | The highest value at which idempotence preserves order |
| `compression.type` | lz4 | lz4 | lz4 | See topic design |
| `linger.ms` | 5 | 1 | 20 | Follows each service's latency target: tightest for fraud, loosest for notifications |
| `batch.size` | 65536 | 32768 | 65536 | Small enough to keep memory modest, and well under the 1 MiB limit |
| `delivery.timeout.ms` | 30000 | 10000 | 60000 | How long a record may stay unsent |
| `request.timeout.ms` | 5000 | 3000 | 10000 | Detects a stuck broker well inside the delivery timeout |

The payment producer also sets `max.block.ms=30000`, because `send()` can block waiting for metadata. The default of 60 seconds would break the 30-second limit below.

**Idempotency.** Duplicate payments must not be possible, and this needs two layers. The idempotent producer removes duplicates from network retries within a producer session, but it can't help if an application retries or a consumer reprocesses. So every event also has a stable `eventId`. Fraud and notification IDs are derived from the source event's ID, which makes a reprocessed event produce the same ID.

**Serialization: JSON using `StringSerializer`.** This matches the course templates and makes evidence readable, with `schemaVersion` to manage change. The costs are larger messages and no enforced schema. Avro with a Schema Registry would be the production choice, giving compact messages and compatibility checks. Protobuf is similar, though Avro integrates more commonly with Kafka tooling.

**Error handling.** A payment may be stuck for at most 30 seconds. After that the API returns `503` and logs the payment as unsent, so the caller knows it wasn't confirmed. This favours retrying over failing fast, because short broker failovers are normal and shouldn't fail a payment. Each journey also waits for one stage's acknowledgement before sending the next. Section 7 covers a gap this leaves, which a transactional outbox would close.

**Mock data.** There are 4 fixed journeys, one per possible ending, plus a seeded simulator. Standard runs used 1,000 journeys (3,782 events), split across `MOBILE`, `WEB`, `POS` and `API` channels and `CARD`, `EFT`, `INSTANT_EFT` and `WALLET` types. Rejected and failed payments are deliberately included at about 17% of journeys, and occasional large and foreign payments exercise all fraud levels. Across all tests the system handled 7,079 payments and 26,600 payment events.

### 1.3 Consumer design

| | Fraud | Notification | Audit |
|---|---|---|---|
| **Group** | `fraud-scoring-group` | `notification-sender-group` | `audit-archiver-group` |
| **Purpose** | Score payments in real time | Tell customers the outcome | Keep the compliance archive |
| **Lag SLA** | < 50ms | < 2s | < 1 hour |
| **Why that SLA** | The brief's target, so a score exists before later stages | A person is waiting, and a second or two is unnoticeable | Regulators need completeness, not speed |
| **Reads** | `payments.events.v1` | `payments.events.v1` | `payments.events.v1`, `fraud.scores.v1` |
| **Filter** | `PAYMENT_INITIATED` | `PAYMENT_VALIDATED` and failures | None, keeps everything |
| **Action** | Score with rules, publish to `fraud.scores.v1` | Send mock SMS or push, publish to `notifications.events.v1` | Append to a file, fsync, verify ordering |
| **Output** | `SCORED` log, score event | `NOTIFIED` log, notification event | JSONL archive, `/audit/verify` report |
| **Instances run in POC** | 2 | 2 | 1 |
| **Instances at peak** | Up to 100 | Up to 100 | Up to 130, if the archive moves to shared storage |

**Filtering.** Consumers check the `event-type` header first, skipping irrelevant records without parsing them. A record with no header didn't come from our producer, so it's parsed and judged on its contents. This is what lets a malformed record reach a DLQ rather than being silently skipped.

**Fraud scoring.** The rules are stateless, so scoring needs no I/O and adds almost no latency. The score adds +50 for a very large amount (≥ R50,000) or +25 for a large one (≥ R10,000), +30 for a high-risk country or +20 for any other foreign country, and +15 for an unknown device. Under 30 is low, 30–59 is medium, and 60 or more is high.

**Consumer settings.**

| Setting | Fraud | Notification | Audit | Why |
|---|---|---|---|---|
| `max.poll.records` | 50 | 200 | 500 | Small batches for fast turnaround, large for archive efficiency |
| `session.timeout.ms` | 10000 | 30000 | 45000 | Fraud detects a dead instance fastest, because a stalled partition hurts it most |
| `heartbeat.interval.ms` | 3000 | 10000 | 15000 | About a third of the session timeout |
| `auto.offset.reset` | earliest | earliest | earliest | A new group must not skip payments |
| `enable.auto.commit` | false | false | false | Auto-commit could mark events done before their work is durable |
| `max.poll.interval.ms` | 30000 | 120000 | 300000 | Allows for each service's slowest expected batch |
| `fetch.min.bytes` | 1 | 1024 | 65536 | Respond immediately for fraud, batch up for audit |
| `fetch.max.wait.ms` | 10 | 100 | 500 | The most latency each service will trade for fuller fetches |
| `max.partition.fetch.bytes` | 262144 | 1048576 | 1048576 | Keeps fraud's fetches small and quick |
| `partition.assignment.strategy` | CooperativeSticky | CooperativeSticky | CooperativeSticky | Rebalances move only the partitions that must move |

**Offsets and delivery semantics.** All three groups use at-least-once delivery, committing manually only after their work is durable: after Kafka acknowledges every downstream write, or after an fsync for audit. Missing an event is never acceptable, but duplicates can be handled.

| If an event is processed twice | Handling |
|---|---|
| Fraud | Publishes a second score with the same derived `eventId`, which downstream systems can recognise |
| Notification | Skipped using an in-memory set of 100,000 source event IDs. A restart clears it, so production would need Redis or a database |
| Audit | Skipped by topic, partition and offset, which survives restarts because it's rebuilt from the file |

Kafka transactions could give exactly-once processing for fraud and notification output, but their commit interval would use up much of the 50ms budget, and a notification sent through an external gateway can't be part of one anyway.

**Error handling.** Bad records go to the group's DLQ with their origin and error in headers, then the group moves on, so one bad record can't block a partition. Notification gateway failures retry 3 times before going to the DLQ. Errors writing to Kafka itself are different: the batch stays uncommitted and retries indefinitely, because skipping it would lose events. Spring's default gives up after 9 retries, so this was overridden.

**Scalability.** Fraud and notification scale horizontally by adding instances, up to one per partition. Audit runs as a single instance because the POC archive is one file on a single-writer volume. Production would move the archive to object storage, which allows more instances.

---

## 2. Topic Creation

### Start the cluster

Kubernetes must be enabled in Docker Desktop with the kubeadm cluster type.

```bash
kubectl apply -f k8s/kafka.yaml
kubectl rollout status statefulset/kafka --timeout=270s
bash scripts/create-topics.sh
bash scripts/build-images.sh        # also runs the 20 unit tests
kubectl apply -f k8s/services.yaml
```

Output from `kafka-metadata-quorum describe --status`, showing all 3 brokers as voters:

```
ClusterId:              q1Sh-9_ISia_zwGINzRvyQ
LeaderId:               1
LeaderEpoch:            235
HighWatermark:          10745
MaxFollowerLag:         0
MaxFollowerLagTimeMs:   338
CurrentVoters:          [0,1,2]
CurrentObservers:       []
```

### Create the topics

The full commands are in [`payment.md`](payment.md), [`fraud.md`](fraud.md) and [`notification.md`](notification.md), and were run exactly as written against temporary topics ([`evidence/14-template-commands.txt`](evidence/14-template-commands.txt)). The payments topic:

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

The three DLQs use 3 partitions, because they should be nearly empty, and 7-year retention, because they can hold regulated events.

### Verify with `--describe`

```
Topic: payments.events.v1	PartitionCount: 100	ReplicationFactor: 3	Configs: compression.type=lz4,min.insync.replicas=2,cleanup.policy=delete,retention.ms=220924800000,max.message.bytes=1048576,retention.bytes=-1
	Topic: payments.events.v1	Partition: 0	Leader: 2	Replicas: 1,2,0	Isr: 2,1,0
	Topic: payments.events.v1	Partition: 1	Leader: 2	Replicas: 2,0,1	Isr: 2,1,0
	Topic: payments.events.v1	Partition: 2	Leader: 1	Replicas: 0,1,2	Isr: 2,1,0
	...
Topic: fraud.scores.v1	PartitionCount: 30	ReplicationFactor: 3	Configs: compression.type=lz4,min.insync.replicas=2,cleanup.policy=delete,retention.ms=220924800000,max.message.bytes=1048576,retention.bytes=-1
Topic: notifications.events.v1	PartitionCount: 30	ReplicationFactor: 3	Configs: compression.type=lz4,min.insync.replicas=2,cleanup.policy=delete,retention.ms=2592000000,max.message.bytes=1048576,retention.bytes=-1
```

The full output for all 6 topics is in [`evidence/run4-quiet-rate20/02-topics.txt`](evidence/run4-quiet-rate20/02-topics.txt).

---

## 3. Producer Setup

The real producers are the services, chosen over the console producer because a journey has to wait for each stage's acknowledgement, which needs logic a console tool doesn't have. The console commands are in the three template files, and were used for the poison message test.

To produce events:

```bash
kubectl exec deploy/payment-service -- curl -s -X POST localhost:8080/scenarios
kubectl exec deploy/payment-service -- curl -s -X POST "localhost:8080/simulate?count=1000&seed=42&ratePerSec=20"
```

[`postman/payment-events.postman_collection.json`](postman/payment-events.postman_collection.json) has the same requests, with variables and built-in checks. Its 12 requests and 23 checks all passed in Newman.

### Evidence of production

Simulator summary from the quiet 20 journeys/s run:

```json
{"journeysRequested":1000,"seed":42,"ratePerSec":20,"eventsAcknowledged":3782,"journeysUnsent":0,
 "outcomes":{"COMPLETED":831,"NOT_AUTHORISED":80,"INVALIDATED":58,"NOT_COMPLETED":31},"durationMs":49991}
Records added to payments.events.v1: 3782
```

Complete journeys with partitions and offsets, showing a successful one and a failed one:

```json
{"paymentId":"2903a2f6-1365-4770-bf06-92e9dc3a64be","outcome":"COMPLETED","events":[
  {"eventType":"PAYMENT_INITIATED","partition":1,"offset":166},
  {"eventType":"PAYMENT_AUTHORISED","partition":1,"offset":167},
  {"eventType":"PAYMENT_VALIDATED","partition":1,"offset":168},
  {"eventType":"PAYMENT_COMPLETED","partition":1,"offset":169}]}

{"paymentId":"aa199ac5-063b-45b0-ac5f-9c139f1834c8","outcome":"NOT_AUTHORISED","events":[
  {"eventType":"PAYMENT_INITIATED","partition":23,"offset":134},
  {"eventType":"PAYMENT_NOT_AUTHORISED","partition":23,"offset":135}]}
```

### Example events

`PAYMENT_INITIATED`:

```json
{"eventId":"a5c7a283-bdb4-45e4-9757-9a4a4c56e65e","schemaVersion":1,"eventType":"PAYMENT_INITIATED","paymentId":"3ce95ecb-5ed5-432d-bed6-58e5051f5540","customerId":"cust-1","accountId":"acc-1","merchantId":"merch-1","amountMinor":25000,"currency":"ZAR","channel":"MOBILE","paymentType":"CARD","deviceId":"device-1","ipCountry":"ZA","reasonCode":null,"occurredAt":"2026-11-01T11:41:07.356793434Z","correlationId":"1c95b924-ef87-4a80-ba50-c8a97ae1213d"}
```

`PAYMENT_NOT_COMPLETED`, from a high-risk payment:

```json
{"eventId":"f51d0420-d79e-4a38-90a0-312759b7d354","schemaVersion":1,"eventType":"PAYMENT_NOT_COMPLETED","paymentId":"d32b6964-e71b-4ef5-8dbb-10d0973ff156","customerId":"cust-4","accountId":"acc-4","merchantId":"merch-4","amountMinor":7500000,"currency":"ZAR","channel":"WEB","paymentType":"CARD","deviceId":null,"ipCountry":"RU","reasonCode":"RAIL_TIMEOUT","occurredAt":"2026-11-01T11:24:01.678373303Z","correlationId":"dd9e528c-6b28-4405-8831-29949a6f76a1"}
```

### Decisions made during testing

- **Steady-rate simulator:** journeys are paced at a set rate, so lag reflects sustained load and not one burst.
- **Warm-up before measuring:** each run sends 100 journeys and then resets the latency counters, so JVM warm-up doesn't count against the SLAs.

---

## 4. Consumer Groups

The groups are created when the services start and subscribe. Their settings are in each service's `src/main/resources/application.yml`, under the exact Kafka property names.

```bash
kubectl apply -f k8s/services.yaml
```

To inspect a topic without disturbing a service, use a console consumer with **its own group**, because joining the service's group would take partitions away from it:

```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic fraud.scores.v1 \
  --group fraud-scores-console \
  --from-beginning \
  --property print.key=true --property print.partition=true --property print.offset=true
```

### Fraud: `fraud-scoring-group`

Filters to `PAYMENT_INITIATED` and scores each one. The four fixed journeys show the full range of results:

```
SCORED paymentId=0c01984f-... score=0  type=FRAUD_SCORE_LOW    reasons=[] from=payments.events.v1-35@0
SCORED paymentId=9c50bec4-... score=25 type=FRAUD_SCORE_LOW    reasons=[LARGE_AMOUNT] from=payments.events.v1-33@0
SCORED paymentId=905c434c-... score=45 type=FRAUD_SCORE_MEDIUM reasons=[LARGE_AMOUNT, CROSS_BORDER] from=payments.events.v1-66@0
SCORED paymentId=d32b6964-... score=95 type=FRAUD_SCORE_HIGH   reasons=[VERY_LARGE_AMOUNT, HIGH_RISK_COUNTRY, UNKNOWN_DEVICE] from=payments.events.v1-52@0
```

Resulting record on `fraud.scores.v1`:

```json
{"eventId":"057bec83-07f0-3d69-9663-0d6c75625955","schemaVersion":1,"eventType":"FRAUD_SCORE_LOW","paymentId":"17292978-ed73-4f78-be4f-651c9807b4b9","customerId":"cust-1","score":0,"reasons":[],"occurredAt":"2026-11-01T11:32:28.199822822Z","correlationId":"fa734f6a-849f-4757-9e92-ef03a093fcc3","causationId":"ae28942a-7faa-4777-888b-8ba31a811995"}
```

### Notification: `notification-sender-group`

Filters to `PAYMENT_VALIDATED` and failures. The mock gateway fails 2% of attempts on purpose, so the retry path runs during normal operation. It retried 76 times in the logs of the current pods, and none of those reached the DLQ:

```
GATEWAY_FAILURE paymentId=905c434c-... attempt=1/3
NOTIFIED customerId=cust-3 paymentId=905c434c-... attempt=2 message="Your payment failed (INVALID_BENEFICIARY)."
```

A payment that passes validation and then fails completion gets two messages, which follows from the brief's choice of events:

```
NOTIFIED customerId=cust-4 paymentId=d32b6964-... attempt=1 message="Your payment was approved."
NOTIFIED customerId=cust-4 paymentId=d32b6964-... attempt=1 message="Your payment failed (RAIL_TIMEOUT)."
```

Resulting record on `notifications.events.v1`:

```json
{"eventId":"d1797de9-2569-3266-91c9-dd6b6ad5e0d8","schemaVersion":1,"eventType":"NOTIFICATION_PAYMENT_APPROVED","paymentId":"4a5601b8-7711-46dc-94a7-70d91a4f10a4","customerId":"cust-813","deliveryChannel":"PUSH","message":"Your payment was approved.","occurredAt":"2026-11-01T11:32:31.740212267Z","correlationId":"4dee7b02-701d-40bb-8759-737548f774db","causationId":"ced955e0-8e45-4c47-b8ff-98d936091729"}
```

### Audit: `audit-archiver-group`

Keeps all events from both topics in an append-only file and commits only after an fsync. No transformation is applied beyond wrapping each event with its topic, partition, offset and archive time.

```
ARCHIVED count=20
ARCHIVED count=3
```

---

## 5. Verification

### Event ordering

`GET /audit/verify` checks every archived payment for three things: one partition only, increasing offsets, and legal state transitions. These are the results after all tests:

```json
{"payments":7079,"events":26600,
 "endings":{"PAYMENT_NOT_AUTHORISED":555,"PAYMENT_INVALIDATED":410,"PAYMENT_COMPLETED":5769,"PAYMENT_NOT_COMPLETED":245},
 "inProgress":100,"violationCount":0,"violations":[]}
```

**There were 0 violations across 7,079 payments.** The 100 in progress are journeys cut short by the outage in Run 3 (see section 7), not ordering errors.

One payment's events read directly from its partition, confirming they stay together with consecutive offsets:

```
Partition:40	Offset:0	event-type:PAYMENT_INITIATED	3ce95ecb-5ed5-432d-bed6-58e5051f5540
Partition:40	Offset:1	event-type:PAYMENT_AUTHORISED	3ce95ecb-5ed5-432d-bed6-58e5051f5540
Partition:40	Offset:2	event-type:PAYMENT_VALIDATED	3ce95ecb-5ed5-432d-bed6-58e5051f5540
Partition:40	Offset:3	event-type:PAYMENT_COMPLETED	3ce95ecb-5ed5-432d-bed6-58e5051f5540
```

### Consumer group status and lag

`kafka-consumer-groups --describe` shows all 3 groups stable and fully caught up, with the audit group covering 130 partitions because it reads two topics:

```
fraud-scoring-group        partitions 100  lag 0   Stable, 2 members
notification-sender-group  partitions 100  lag 0   Stable, 2 members
audit-archiver-group       partitions 130  lag 0   Stable, 1 member
```

Sample rows, as `GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG`, showing all three groups independently at the same offset:

```
fraud-scoring-group       payments.events.v1 1 202 202 0
notification-sender-group payments.events.v1 1 202 202 0
audit-archiver-group      payments.events.v1 1 202 202 0
audit-archiver-group      fraud.scores.v1    0 167 167 0
```

### Latency against the SLAs

Kafka reports lag as a count of offsets, but the SLAs are in milliseconds. So each service records the time since `occurredAt`. A consumer can only read a record after it's replicated, so these figures include the producer and replication time as well as the consumer's own.

**1,000 journeys, on an otherwise quiet machine.** Ranges cover the two instances of each service.

| Service | Target | Rate | p50 | p95 | p99 | Max |
|---|---|---|---|---|---|---|
| Payment | 150ms | 20/s | **13ms** | 157ms | 1,252ms | 2,743ms |
| Fraud | 50ms | 20/s | **14–15ms** | 805–868ms | 2,376–2,837ms | 3,031ms |
| Notification | 2s | 20/s | **29–30ms** | **199–292ms** | **573–1,016ms** | 3,104ms |
| Audit | 1 hour | 20/s | **281ms** | **562ms** | **1,787ms** | **3,370ms** |
| Payment | 150ms | 100/s | 161ms | 497ms | 740ms | 854ms |
| Fraud | 50ms | 100/s | 510–1,112ms | 1,011–1,877ms | 1,198–2,132ms | 2,314ms |
| Notification | 2s | 100/s | **446–521ms** | **900–914ms** | **1,070–1,168ms** | **1,422ms** |
| Audit | 1 hour | 100/s | **463ms** | **865ms** | **1,213ms** | **1,528ms** |

Bold figures are within target. Typical latency at the lower rate meets every target, but the fraud target isn't met at p95, and the payment target narrowly misses at p95. These tests run 9 pods on one laptop, so they can't show what a real cluster with dedicated hardware would do.

### Failure tests

| Test | What happened | Evidence |
|---|---|---|
| **One broker down** during 600 journeys | All 100 partitions dropped to 2 in-sync replicas, leaders moved, and no journey failed. Acknowledged 2,275 events, and exactly 2,275 reached the topic. | [`09`](evidence/09-one-broker-down.txt) |
| **Two brokers down** | The payment request returned `503` after 30.3 seconds, matching the designed limit. Nothing was written for it. After recovery a new payment succeeded in 0.19 seconds and all groups returned to lag 0. | [`10`](evidence/10-two-brokers-down.txt) |
| **Malformed message** | The record went to all 3 DLQs with its origin and error attached, and the 4 valid journeys sent straight after it processed normally. | [`11`](evidence/11-poison-message.txt) |
| **Fraud pod killed** during 600 journeys | The group was stable with 1 member at the next reading, the replacement joined, and lag returned to 0. No journey failed and no duplicate scores were added. | [`12`](evidence/12-fraud-pod-killed.txt) |

Under one broker down, as the design expects, partitions showed reduced ISR but stayed writable:

```
Topic: payments.events.v1	Partition: 0	Leader: 1	Replicas: 1,2,0	Isr: 1,0
Topic: payments.events.v1	Partition: 1	Leader: 0	Replicas: 2,0,1	Isr: 1,0
payments.events.v1 under-replicated partitions: 100
```

**Caveat on the two-broker test.** A rejection of `NotEnoughReplicas` was expected, but this test can't confirm that was the cause. With 2 of 3 brokers gone, the KRaft controller also loses its majority, so the failure isn't only about in-sync replicas. The service's log recorded only `KafkaProducerException: Failed to send`, without the underlying cause. The behaviour that matters did hold: the write was refused rather than falsely confirmed.

### Final reconciliation

Each count can be checked against another, which is the strongest evidence that nothing was lost ([`evidence/15-final-state.txt`](evidence/15-final-state.txt)):

| Check | Result |
|---|---|
| Records on `payments.events.v1` | 26,601 = 26,600 archived + 1 malformed message in the DLQ |
| Lines in the audit file | 33,741 = 26,600 payment + 7,141 fraud records |
| Unique fraud scores | 7,079 = the 7,079 payments, exactly one each |
| Duplicate fraud records | 62, out of 7,141 total. All share an `eventId` with an original, so they can be recognised |
| Duplicate notification records | 0, out of 7,252 |

---

## 6. Trade-offs & Justifications

| Decision | Benefit | Cost | Why this way |
|---|---|---|---|
| One payments topic, not one per event type | Full ordering per payment | Consumers fetch and discard events they don't need | Ordering is a hard requirement, and fetch cost can be scaled |
| `paymentId` as key, not `customerId` | Even spread, no hot partitions | No ordering across a customer's payments, and velocity rules need a separate store | Sizing leaves no slack for skew |
| 100 partitions | Reaches peak with 10 Mb/s consumers | More file handles, metadata and failover work | Set by the consumer limit in the brief, and hard to change later |
| `acks=all` with min ISR 2 | Survives a broker loss with no acknowledged data lost | Each write waits for replication | Payments can't be lost |
| At-least-once, not exactly-once | Simpler and faster, and no events missed | Duplicates are possible, and 62 occurred | Stable IDs make duplicates harmless, and transactions can't cover external gateways |
| Non-blocking fraud scoring | Payment latency is independent of fraud | Risky payments can proceed before their score is seen | Follows the brief's flow, but production should gate on high scores |
| JSON, not Avro | Readable evidence, and matches the templates | Larger messages and no enforced schema | Right for a POC, but Avro is the production choice |
| Time-only retention | Regulated data can't be deleted early by a size cap | Disk use has to be monitored | Compliance outweighs operational convenience |
| Waiting for each stage's acknowledgement | A payment can't advance on an unstored state | Slower journeys | Correctness comes first for payment state |
| Manual commit after durable work | No event is marked done too early | More code, and redelivery after failure | Auto-commit can lose events on a crash |

---

## 7. Issues Encountered

### Template commands didn't work as written

The first four defects were tested directly ([`evidence/13-template-defects.txt`](evidence/13-template-defects.txt)). The last two come from how these tools are documented to behave and weren't tested separately. The corrected versions were run from the template files ([`evidence/14-template-commands.txt`](evidence/14-template-commands.txt)).

| Defect | What happened | Fix |
|---|---|---|
| Client settings passed with `--property` | **Silently ignored.** `acks=banana` was accepted and `session.timeout.ms=1` had no effect, so the command appears to work with none of the chosen settings | `--producer-property` and `--consumer-property`, which reject those values |
| `parse.key=true` on the console consumer | The key isn't shown, because that option belongs to the producer | `print.key=true` |
| A space after a line-ending `\` | Bash ends the command there and tries to run the next line as its own command | Removed |
| `retention.ms= <value>` with a space | `Invalid topic config`, because it's read as two arguments | No spaces |
| Serializer properties on the console producer | The console producer sends bytes regardless | Removed |
| Reusing a service's group on a console consumer | Would take partitions from the running service | A separate group for each console consumer |

### Latency was far over target, and the first fix didn't help

This took the most effort to resolve, and the first explanation was wrong. All runs used 1,000 journeys.

| Run | Conditions | Payment p50 | Fraud p50 | Controller elections | Unsent journeys |
|---|---|---|---|---|---|
| 1 | Busy machine, default timeouts, 100/s | 1,334ms | 7,544–10,611ms | 322 (cumulative) | 0 |
| 2 | Busy machine, longer timeouts, 100/s | 1,554ms | 26,952–30,444ms | 1 | 0 |
| 3 | Busy machine, longer timeouts, 20/s | 394ms | 3,132–4,896ms | 17 | 187 |
| 4 | Quiet machine, 20/s | **13ms** | **14–15ms** | 2 | 0 |
| 5 | Quiet machine, 100/s | 161ms | 510–1,112ms | 2 | 0 |

1. **Run 1:** Latency was far over target, with hundreds of KRaft controller elections and 54 duplicate fraud scores. A fraud consumer also lost its group membership (`UNKNOWN_MEMBER_ID`).
2. **Run 2, the wrong fix:** Elections looked like the cause, so `controller.quorum.fetch.timeout.ms`, `controller.quorum.election.timeout.ms` and `broker.session.timeout.ms` were raised. Elections fell to 1 and membership losses stopped, **but latency got no better.** So elections were a symptom, not the main cause.
3. **Isolating the bottleneck:** A raw `kafka-producer-perf-test` at only 100 records/s, with my producer code out of the path, still showed a 332ms median and an 8 second p99. With the services stopped as well, the median fell to 15ms, though 400 records/s still stalled. Windows was at 100% CPU, and the VM's load average was about 18 on 8 CPUs.
4. **Finding the contention:** An unrelated set of containers on the same machine included one crash-looping service, with 197 restarts. Stopping that stack dropped the raw benchmark at 1,000 records/s to a 25ms median and 135ms at p95, and Runs 4 and 5 followed.

**Remaining open points:**

- **What's still causing the tail:** the quiet runs still have stalls of 1–3 seconds at p99. The Windows CPU was still around 50% after the stack stopped, and the cause of those stalls wasn't identified.
- **Why fraud is slowest at 100/s:** fraud has the tightest settings but the worst lag at this rate. The likely reason is that each poll of up to 50 records waits for `acks=all` on its scores before fetching more, so slow acknowledgements limit throughput. This wasn't tested. Making that wait asynchronous would be the first thing to try.
- **Timeouts are laptop settings:** the longer broker timeouts are in `k8s/kafka.yaml` and aren't recommended for production, where slower failure detection has a real cost.

### Broker killed by a load test

Running the benchmark through `kubectl exec` started an extra JVM inside the broker's container, which pushed it past its 1 GiB limit, and Kubernetes killed it (`OOMKilled`, exit 137). The limit was raised to 1.5 GiB, and later benchmarks ran from a separate pod. Replication meant the cluster recovered without any data loss.

### A timed-out write can still succeed

Run 3 had 187 journeys reported as unsent, with 3,070 events acknowledged, yet **3,367** records reached the topic. So 297 records were stored without their acknowledgement reaching the application in time. A timeout means "unknown", not "failed".

This also left 100 journeys permanently incomplete, because a journey stops at its failed stage. The audit report shows them as `inProgress`, which is why that figure is 100 throughout. It's a real gap in the design: retrying could duplicate a state change, and not retrying leaves a payment unfinished. A transactional outbox, which records the intended event in a database first, is the standard fix. Reconciling against the audit archive would catch whatever is left.

### Duplicate fraud scores

The 62 duplicate scores came from the earlier unstable runs: 54 in Run 1 and 8 more before Run 4. A rebalance can redeliver a batch whose offsets weren't yet committed, which is expected with at-least-once delivery. Every duplicate has the same derived `eventId` as its original, so the design handled it as intended. The failure tests added none.

### Smaller issues

- **`kafka-topics` warning about periods in names:** it warns that `.` and `_` can collide in metric names. These topic names use periods and hyphens but no underscores, so they can't collide, and the warning was accepted.
- **Slow service startup:** payment-service took about 80 seconds to start on the busy machine, which made a 120-second rollout check time out even though the pod was healthy.
- **Coarse lag readings:** `kafka-consumer-groups` takes seconds to run, so its readings can't show sub-second lag. That's why time-based measurement was added.

---

## Conclusions

### What this POC shows

- **Correctness holds under failure.** Ordering, durability and reconciliation all held through broker and pod failures.
- **Latency depends heavily on the environment.** Typical latency meets the targets at the rate tested on a quiet machine, but tail latency and the 50ms fraud target don't. These can't be judged properly on a laptop.
- **Peak throughput is calculated, not proven.** The 100-partition design is sized for 1000 Mb/s, which would be about 272,000 events/s at 460 bytes each. The highest full-system load run was about 1.3 Mb/s, roughly 0.13% of that. Proving it needs a real load test on dedicated brokers.

### Next steps for production

1. Run a load test on dedicated hardware to measure real throughput and tail latency.
2. Remove the blocking wait on fraud's writes, and test whether that fixes its lag under load.
3. Add a transactional outbox to close the timed-out write gap.
4. Gate authorisation on high fraud scores.
5. Move to Avro with a Schema Registry.
6. Move the audit archive to object storage and add tiered storage for the 7-year retention.
7. Keep notification deduplication in Redis or a database.
8. Log the root cause of producer failures, which the two-broker test showed is missing.
9. Add authentication, encryption and access control to Kafka, which this POC doesn't have.

### What I learned

> **TODO, in your own words before submitting:** what did you learn, and what would you do differently? This section is deliberately left for you.

---

## Evidence index

| Path | Contents |
|---|---|
| `evidence/run1-default-timeouts/` to `run3-rate20/` | The slow runs on the busy machine, kept to show the diagnosis |
| `evidence/run4-quiet-rate20/`, `run5-quiet-rate100/` | The main results |
| `evidence/09-` to `12-` | Failure tests |
| `evidence/13-template-defects.txt` | Tests of each template defect |
| `evidence/14-template-commands.txt` | The corrected template commands, run as written |
| `evidence/15-final-state.txt` | Final reconciliation |

Evidence is unedited, except for a local file path shortened in `13-template-defects.txt` to remove a username.
