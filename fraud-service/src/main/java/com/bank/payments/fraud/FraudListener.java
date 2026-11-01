package com.bank.payments.fraud;

import com.bank.payments.common.DlqPublisher;
import com.bank.payments.common.EventIds;
import com.bank.payments.common.FraudEvent;
import com.bank.payments.common.LatencyTracker;
import com.bank.payments.common.PaymentEvent;
import com.bank.payments.common.PaymentEventType;
import com.bank.payments.common.Topics;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Component
public class FraudListener {
    private static final Logger log = LoggerFactory.getLogger(FraudListener.class);
    private static final String DLQ = "fraud-scoring-group.dlq";
    private static final byte[] INITIATED = PaymentEventType.PAYMENT_INITIATED.name().getBytes(StandardCharsets.UTF_8);

    private final FraudScorer scorer = new FraudScorer();
    private final KafkaTemplate<String, String> kafka;
    private final DlqPublisher dlq;
    private final ObjectMapper mapper;
    private final LatencyTracker latency;

    public FraudListener(KafkaTemplate<String, String> kafka, DlqPublisher dlq, ObjectMapper mapper,
                         LatencyTracker latency) {
        this.kafka = kafka;
        this.dlq = dlq;
        this.mapper = mapper;
        this.latency = latency;
    }

    /**
     * At-least-once: offsets are committed only after every score and DLQ write in the poll has been
     * acknowledged. If one fails, the exception leaves the batch uncommitted and it is retried.
     */
    @KafkaListener(topics = Topics.PAYMENTS, groupId = "${spring.kafka.consumer.group-id}")
    public void onBatch(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        List<CompletableFuture<?>> writes = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            if (!isCandidate(record)) {
                continue;
            }
            try {
                PaymentEvent event = mapper.readValue(record.value(), PaymentEvent.class).requireValid();
                if (event.eventType() == PaymentEventType.PAYMENT_INITIATED) {
                    latency.recordSince("consumer-lag", event.occurredAt());
                    writes.add(publishScore(event, record));
                }
            } catch (JsonProcessingException | IllegalArgumentException e) {
                writes.add(dlq.publish(DLQ, record, e));
            }
        }
        CompletableFuture.allOf(writes.toArray(CompletableFuture[]::new)).join();
        ack.acknowledge();
    }

    /** A missing header means the record did not come from our producer, so it must be parsed to judge it. */
    private static boolean isCandidate(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(Topics.HEADER_EVENT_TYPE);
        return header == null || java.util.Arrays.equals(header.value(), INITIATED);
    }

    private CompletableFuture<?> publishScore(PaymentEvent source, ConsumerRecord<String, String> record)
            throws JsonProcessingException {
        FraudScorer.Result result = scorer.score(source);
        FraudEvent event = new FraudEvent(
                EventIds.derive(source.eventId(), "fraud-score"), 1, result.type(), source.paymentId(),
                source.customerId(), result.score(), result.reasons(), Instant.now(), source.correlationId(),
                source.eventId());

        ProducerRecord<String, String> out = new ProducerRecord<>(
                Topics.FRAUD, source.paymentId(), mapper.writeValueAsString(event));
        out.headers().add(Topics.HEADER_EVENT_TYPE, result.type().name().getBytes(StandardCharsets.UTF_8));

        log.info("SCORED paymentId={} score={} type={} reasons={} from={}-{}@{}", source.paymentId(), result.score(),
                result.type(), result.reasons(), record.topic(), record.partition(), record.offset());
        return kafka.send(out);
    }
}
