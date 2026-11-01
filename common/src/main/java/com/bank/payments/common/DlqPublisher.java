package com.bank.payments.common;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/** Parks an unprocessable record with its origin, so one bad record cannot block a partition. */
@Component
public class DlqPublisher {
    private static final Logger log = LoggerFactory.getLogger(DlqPublisher.class);

    private final KafkaTemplate<String, String> kafka;

    public DlqPublisher(KafkaTemplate<String, String> kafka) {
        this.kafka = kafka;
    }

    public CompletableFuture<?> publish(String dlqTopic, ConsumerRecord<String, String> source, Exception cause) {
        log.warn("DLQ topic={} source={}-{}@{} error={}", dlqTopic, source.topic(), source.partition(),
                source.offset(), cause.toString());
        ProducerRecord<String, String> record = new ProducerRecord<>(dlqTopic, source.key(), source.value());
        header(record, "dlq-source-topic", source.topic());
        header(record, "dlq-source-partition", String.valueOf(source.partition()));
        header(record, "dlq-source-offset", String.valueOf(source.offset()));
        header(record, "dlq-error", cause.toString());
        return kafka.send(record);
    }

    private static void header(ProducerRecord<String, String> record, String name, String value) {
        record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
    }
}
