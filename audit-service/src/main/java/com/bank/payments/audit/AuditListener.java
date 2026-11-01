package com.bank.payments.audit;

import com.bank.payments.common.DlqPublisher;
import com.bank.payments.common.FraudEvent;
import com.bank.payments.common.LatencyTracker;
import com.bank.payments.common.PaymentEvent;
import com.bank.payments.common.Topics;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Component
public class AuditListener {
    private static final Logger log = LoggerFactory.getLogger(AuditListener.class);
    private static final String DLQ = "audit-archiver-group.dlq";

    private final AuditLog auditLog;
    private final DlqPublisher dlq;
    private final ObjectMapper mapper;
    private final LatencyTracker latency;

    public AuditListener(AuditLog auditLog, DlqPublisher dlq, ObjectMapper mapper, LatencyTracker latency) {
        this.auditLog = auditLog;
        this.dlq = dlq;
        this.mapper = mapper;
        this.latency = latency;
    }

    /** Archives all events, with no filtering. Offsets are committed only after an fsync. */
    @KafkaListener(topics = {Topics.PAYMENTS, Topics.FRAUD}, groupId = "${spring.kafka.consumer.group-id}")
    public void onBatch(List<ConsumerRecord<String, String>> records, Acknowledgment ack) throws IOException {
        List<CompletableFuture<?>> dlqWrites = new ArrayList<>();
        int archived = 0;
        for (ConsumerRecord<String, String> record : records) {
            try {
                ParsedEvent event = parse(record);
                if (auditLog.append(record.topic(), record.partition(), record.offset(), record.key(), event.type(),
                        event.json())) {
                    archived++;
                    latency.recordSince("consumer-lag", java.time.Instant.parse(event.json().path("occurredAt").asText()));
                }
            } catch (JsonProcessingException | IllegalArgumentException | java.time.format.DateTimeParseException e) {
                dlqWrites.add(dlq.publish(DLQ, record, e));
            }
        }
        auditLog.sync();
        CompletableFuture.allOf(dlqWrites.toArray(CompletableFuture[]::new)).join();
        ack.acknowledge();
        if (archived > 0) {
            log.info("ARCHIVED count={}", archived);
        }
    }

    private record ParsedEvent(String type, JsonNode json) {
    }

    /** Validates against the real schema, so a bad record never reaches the regulatory archive. */
    private ParsedEvent parse(ConsumerRecord<String, String> record) throws JsonProcessingException {
        if (record.key() == null) {
            throw new IllegalArgumentException("Missing key");
        }
        JsonNode json = mapper.readTree(record.value());
        if (Topics.PAYMENTS.equals(record.topic())) {
            PaymentEvent event = mapper.treeToValue(json, PaymentEvent.class).requireValid();
            return new ParsedEvent(event.eventType().name(), json);
        }
        FraudEvent event = mapper.treeToValue(json, FraudEvent.class);
        if (event.eventType() == null || event.occurredAt() == null) {
            throw new IllegalArgumentException("Missing eventType or occurredAt");
        }
        return new ParsedEvent(event.eventType().name(), json);
    }
}
