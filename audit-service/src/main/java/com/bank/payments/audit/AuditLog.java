package com.bank.payments.audit;

import com.bank.payments.common.PaymentEventType;
import com.bank.payments.common.Topics;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Append-only JSONL store. Writes are deduplicated by topic-partition-offset, so a batch redelivered after
 * a crash does not produce duplicate lines. State is rebuilt from the file on startup.
 */
@Component
public class AuditLog {
    private static final Logger log = LoggerFactory.getLogger(AuditLog.class);

    private final Path file;
    private final ObjectMapper mapper;
    private final OrderingVerifier verifier = new OrderingVerifier();
    private final Set<String> seen = new HashSet<>();
    private final Map<String, Integer> fraudLevels = new ConcurrentHashMap<>();

    private FileOutputStream stream;
    private BufferedWriter writer;

    public AuditLog(@Value("${audit.file:/data/audit/audit-log.jsonl}") String file, ObjectMapper mapper) {
        this.file = Path.of(file);
        this.mapper = mapper;
    }

    @PostConstruct
    void open() throws IOException {
        Files.createDirectories(file.toAbsolutePath().getParent());
        if (Files.exists(file)) {
            try (Stream<String> lines = Files.lines(file)) {
                lines.forEach(this::restore);
            }
            log.info("AUDIT_RESTORED lines={} file={}", seen.size(), file);
        }
        stream = new FileOutputStream(file.toFile(), true);
        writer = new BufferedWriter(new OutputStreamWriter(stream, StandardCharsets.UTF_8));
    }

    private void restore(String line) {
        try {
            JsonNode node = mapper.readTree(line);
            seen.add(position(node.path("topic").asText(), node.path("partition").asInt(), node.path("offset").asLong()));
            index(node.path("topic").asText(), node.path("key").asText(), node.path("eventType").asText(),
                    node.path("partition").asInt(), node.path("offset").asLong());
        } catch (IOException e) {
            // Only a torn final line from a crash can get here. Kafka redelivers that uncommitted record.
            log.warn("AUDIT_RESTORE_SKIPPED line={}", line);
        }
    }

    /** Buffers one event. The caller must call {@link #sync()} before committing offsets. */
    public synchronized boolean append(String topic, int partition, long offset, String key, String eventType,
                                       JsonNode value) throws IOException {
        if (!seen.add(position(topic, partition, offset))) {
            return false;
        }
        writer.write(mapper.writeValueAsString(Map.of(
                "topic", topic, "partition", partition, "offset", offset, "key", key, "eventType", eventType,
                "archivedAt", Instant.now().toString(), "value", value)));
        writer.newLine();
        index(topic, key, eventType, partition, offset);
        return true;
    }

    /** Flushes and fsyncs. Events are durable here, so this is what makes committing safe. */
    public synchronized void sync() throws IOException {
        writer.flush();
        stream.getFD().sync();
    }

    public OrderingVerifier verifier() {
        return verifier;
    }

    public Map<String, Integer> fraudLevels() {
        return fraudLevels;
    }

    public List<OrderingVerifier.Entry> timeline(String paymentId) {
        return verifier.timeline(paymentId);
    }

    private void index(String topic, String key, String eventType, int partition, long offset) {
        if (Topics.PAYMENTS.equals(topic)) {
            verifier.add(key, new OrderingVerifier.Entry(PaymentEventType.valueOf(eventType), partition, offset));
        } else {
            fraudLevels.merge(eventType, 1, Integer::sum);
        }
    }

    private static String position(String topic, int partition, long offset) {
        return topic + "-" + partition + "@" + offset;
    }

    @PreDestroy
    void close() throws IOException {
        sync();
        writer.close();
    }
}
