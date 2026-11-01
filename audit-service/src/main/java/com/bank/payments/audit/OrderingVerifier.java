package com.bank.payments.audit;

import com.bank.payments.common.PaymentEventType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.bank.payments.common.PaymentEventType.*;

/**
 * Checks the guarantee the partition key exists to provide: all events of a payment sit on one partition,
 * and reading that partition in offset order gives a legal state sequence.
 */
public class OrderingVerifier {
    private static final Map<PaymentEventType, Set<PaymentEventType>> LEGAL_NEXT = new EnumMap<>(Map.of(
            PAYMENT_INITIATED, Set.of(PAYMENT_AUTHORISED, PAYMENT_NOT_AUTHORISED),
            PAYMENT_AUTHORISED, Set.of(PAYMENT_VALIDATED, PAYMENT_INVALIDATED),
            PAYMENT_VALIDATED, Set.of(PAYMENT_COMPLETED, PAYMENT_NOT_COMPLETED)));

    public record Entry(PaymentEventType type, int partition, long offset) {
    }

    private final Map<String, List<Entry>> byPayment = new HashMap<>();

    public synchronized void add(String paymentId, Entry entry) {
        byPayment.computeIfAbsent(paymentId, id -> new ArrayList<>()).add(entry);
    }

    public synchronized List<Entry> timeline(String paymentId) {
        List<Entry> entries = new ArrayList<>(byPayment.getOrDefault(paymentId, List.of()));
        entries.sort(Comparator.comparingLong(Entry::offset));
        return entries;
    }

    public synchronized Map<String, Object> verify() {
        List<String> violations = new ArrayList<>();
        Map<PaymentEventType, Integer> endings = new EnumMap<>(PaymentEventType.class);
        int inProgress = 0;
        int events = 0;

        for (Map.Entry<String, List<Entry>> payment : byPayment.entrySet()) {
            String id = payment.getKey();
            List<Entry> entries = new ArrayList<>(payment.getValue());
            events += entries.size();
            violations.addAll(check(id, entries));

            entries.sort(Comparator.comparingLong(Entry::offset));
            PaymentEventType last = entries.get(entries.size() - 1).type();
            if (last.isTerminal()) {
                endings.merge(last, 1, Integer::sum);
            } else {
                inProgress++;
            }
        }

        Map<String, Object> report = new LinkedHashMap<>();
        report.put("payments", byPayment.size());
        report.put("events", events);
        report.put("endings", endings);
        report.put("inProgress", inProgress);
        report.put("violationCount", violations.size());
        report.put("violations", violations.stream().limit(50).toList());
        return report;
    }

    static List<String> check(String id, List<Entry> unsorted) {
        List<Entry> entries = new ArrayList<>(unsorted);
        List<String> violations = new ArrayList<>();

        long partitions = entries.stream().map(Entry::partition).distinct().count();
        if (partitions > 1) {
            violations.add(id + ": spread across " + partitions + " partitions");
            return violations;
        }

        entries.sort(Comparator.comparingLong(Entry::offset));
        if (entries.get(0).type() != PAYMENT_INITIATED) {
            violations.add(id + ": starts with " + entries.get(0).type());
        }
        for (int i = 1; i < entries.size(); i++) {
            PaymentEventType previous = entries.get(i - 1).type();
            PaymentEventType current = entries.get(i).type();
            if (!LEGAL_NEXT.getOrDefault(previous, Set.of()).contains(current)) {
                violations.add(id + ": illegal transition " + previous + " -> " + current);
            }
        }
        return violations;
    }
}
