package com.bank.payments.common;

import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Time-based lag. Kafka's own lag figure counts offsets, but the SLAs in the brief are in milliseconds,
 * so each service records how long events took to reach it. Keeps the most recent samples per metric.
 */
@Component
public class LatencyTracker {
    private static final int CAPACITY = 200_000;

    private final Map<String, Window> windows = new ConcurrentHashMap<>();

    public void recordSince(String metric, Instant start) {
        record(metric, Duration.between(start, Instant.now()).toNanos() / 1_000L);
    }

    public void record(String metric, long micros) {
        windows.computeIfAbsent(metric, m -> new Window()).add(micros);
    }

    public Map<String, Map<String, Object>> snapshot() {
        Map<String, Map<String, Object>> result = new LinkedHashMap<>();
        windows.forEach((metric, window) -> result.put(metric, window.summary()));
        return result;
    }

    public void reset() {
        windows.clear();
    }

    private static final class Window {
        private final long[] samples = new long[CAPACITY];
        private long total;

        synchronized void add(long micros) {
            samples[(int) (total % CAPACITY)] = micros;
            total++;
        }

        synchronized Map<String, Object> summary() {
            long[] sorted = Arrays.copyOf(samples, (int) Math.min(total, CAPACITY));
            Arrays.sort(sorted);
            Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("count", total);
            summary.put("p50Ms", percentileMs(sorted, 0.50));
            summary.put("p95Ms", percentileMs(sorted, 0.95));
            summary.put("p99Ms", percentileMs(sorted, 0.99));
            summary.put("maxMs", percentileMs(sorted, 1.0));
            return summary;
        }

        private static double percentileMs(long[] sorted, double percentile) {
            if (sorted.length == 0) {
                return 0;
            }
            int index = (int) Math.ceil(percentile * sorted.length) - 1;
            return sorted[Math.max(index, 0)] / 1000.0;
        }
    }
}
