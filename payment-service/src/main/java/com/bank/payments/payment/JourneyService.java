package com.bank.payments.payment;

import com.bank.payments.common.LatencyTracker;
import com.bank.payments.common.PaymentEvent;
import com.bank.payments.common.PaymentEventType;
import com.bank.payments.common.Topics;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class JourneyService {
    private static final Logger log = LoggerFactory.getLogger(JourneyService.class);

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final LatencyTracker latency;

    public JourneyService(KafkaTemplate<String, String> kafka, ObjectMapper mapper, LatencyTracker latency) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.latency = latency;
    }

    public record Sent(PaymentEventType eventType, int partition, long offset) {
    }

    public record JourneyResult(String paymentId, Outcome outcome, List<Sent> events) {
    }

    /** Thrown when Kafka did not acknowledge a stage within delivery.timeout.ms. */
    public static class UnsentPaymentException extends RuntimeException {
        public UnsentPaymentException(String paymentId, PaymentEventType stage, Throwable cause) {
            super("Payment " + paymentId + " unsent at " + stage, cause);
        }
    }

    /**
     * Each stage waits for its acknowledgement before the next is sent, because a payment must not
     * advance on a state change Kafka has not durably stored.
     */
    public JourneyResult run(PaymentRequest request) {
        String paymentId = UUID.randomUUID().toString();
        String correlationId = UUID.randomUUID().toString();
        Outcome outcome = request.outcome() == null ? Outcome.COMPLETED : request.outcome();

        List<Sent> sent = new ArrayList<>();
        for (PaymentEventType stage : outcome.stages()) {
            PaymentEvent event = new PaymentEvent(
                    UUID.randomUUID().toString(), PaymentEvent.SCHEMA_VERSION, stage, paymentId,
                    request.customerId(), request.accountId(), request.merchantId(), request.amountMinor(),
                    request.currency(), request.channel(), request.paymentType(), request.deviceId(),
                    request.ipCountry(), outcome.reasonFor(stage), Instant.now(), correlationId);
            sent.add(publish(event));
        }
        return new JourneyResult(paymentId, outcome, sent);
    }

    private Sent publish(PaymentEvent event) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    Topics.PAYMENTS, event.paymentId(), mapper.writeValueAsString(event));
            record.headers().add(Topics.HEADER_EVENT_TYPE, event.eventType().name().getBytes(StandardCharsets.UTF_8));

            RecordMetadata metadata = kafka.send(record).join().getRecordMetadata();
            latency.recordSince("producer-ack", event.occurredAt());
            log.info("SENT type={} paymentId={} partition={} offset={}", event.eventType(), event.paymentId(),
                    metadata.partition(), metadata.offset());
            return new Sent(event.eventType(), metadata.partition(), metadata.offset());
        } catch (JsonProcessingException | RuntimeException e) {
            log.error("UNSENT type={} paymentId={} error={}", event.eventType(), event.paymentId(), e.toString());
            throw new UnsentPaymentException(event.paymentId(), event.eventType(), e);
        }
    }
}
