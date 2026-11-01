package com.bank.payments.notification;

import com.bank.payments.common.DlqPublisher;
import com.bank.payments.common.EventIds;
import com.bank.payments.common.LatencyTracker;
import com.bank.payments.common.NotificationEvent;
import com.bank.payments.common.PaymentEvent;
import com.bank.payments.common.PaymentEventType;
import com.bank.payments.common.Topics;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@Component
public class NotificationListener {
    private static final String DLQ = "notification-sender-group.dlq";
    private static final int DEDUP_CAPACITY = 100_000;

    private final KafkaTemplate<String, String> kafka;
    private final DlqPublisher dlq;
    private final MockSender sender;
    private final ObjectMapper mapper;
    private final LatencyTracker latency;

    /**
     * Source event ids already notified. It lives in memory, so a restart can resend a notification.
     * Production would keep this in Redis or a database.
     */
    private final Set<String> notified = Collections.newSetFromMap(new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > DEDUP_CAPACITY;
        }
    });

    public NotificationListener(KafkaTemplate<String, String> kafka, DlqPublisher dlq, MockSender sender,
                                ObjectMapper mapper, LatencyTracker latency) {
        this.kafka = kafka;
        this.dlq = dlq;
        this.sender = sender;
        this.mapper = mapper;
        this.latency = latency;
    }

    /** At-least-once: offsets are committed only after every write from this poll is acknowledged. */
    @KafkaListener(topics = Topics.PAYMENTS, groupId = "${spring.kafka.consumer.group-id}")
    public void onBatch(List<ConsumerRecord<String, String>> records, Acknowledgment ack) throws InterruptedException {
        List<CompletableFuture<?>> writes = new ArrayList<>();
        List<String> handled = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            if (!isCandidate(record)) {
                continue;
            }
            try {
                PaymentEvent event = mapper.readValue(record.value(), PaymentEvent.class).requireValid();
                Optional<NotificationEvent.Type> type = NotificationMapper.typeFor(event.eventType());
                if (type.isEmpty() || notified.contains(event.eventId())) {
                    continue;
                }
                latency.recordSince("consumer-lag", event.occurredAt());
                String message = NotificationMapper.messageFor(type.get(), event.reasonCode());
                sender.send(event.customerId(), event.paymentId(), message);
                writes.add(publish(event, type.get(), message));
                handled.add(event.eventId());
            } catch (JsonProcessingException | IllegalArgumentException | MockSender.GatewayException e) {
                writes.add(dlq.publish(DLQ, record, e));
            }
        }
        CompletableFuture.allOf(writes.toArray(CompletableFuture[]::new)).join();
        notified.addAll(handled);
        ack.acknowledge();
    }

    /** Skips irrelevant events cheaply. A missing header means the record must be parsed to judge it. */
    private static boolean isCandidate(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(Topics.HEADER_EVENT_TYPE);
        if (header == null) {
            return true;
        }
        try {
            String name = new String(header.value(), StandardCharsets.UTF_8);
            return NotificationMapper.typeFor(PaymentEventType.valueOf(name)).isPresent();
        } catch (IllegalArgumentException e) {
            return true;
        }
    }

    private CompletableFuture<?> publish(PaymentEvent source, NotificationEvent.Type type, String message)
            throws JsonProcessingException {
        NotificationEvent event = new NotificationEvent(
                EventIds.derive(source.eventId(), "notification"), 1, type, source.paymentId(), source.customerId(),
                "MOBILE".equals(source.channel()) ? "PUSH" : "SMS", message, Instant.now(), source.correlationId(),
                source.eventId());
        ProducerRecord<String, String> out = new ProducerRecord<>(
                Topics.NOTIFICATIONS, source.paymentId(), mapper.writeValueAsString(event));
        out.headers().add(Topics.HEADER_EVENT_TYPE, type.name().getBytes(StandardCharsets.UTF_8));
        return kafka.send(out);
    }
}
