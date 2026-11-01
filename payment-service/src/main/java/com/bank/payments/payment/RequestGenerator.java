package com.bank.payments.payment;

import java.util.List;
import java.util.Random;

/** Generates realistic mock payments. The same seed gives the same mix of requests and outcomes. */
public final class RequestGenerator {
    private static final List<String> CHANNELS = List.of("MOBILE", "WEB", "POS", "API");
    private static final List<String> TYPES = List.of("CARD", "EFT", "INSTANT_EFT", "WALLET");
    private static final List<String> FOREIGN_COUNTRIES = List.of("GB", "US", "NG", "RU");

    private final Random random;

    public RequestGenerator(long seed) {
        this.random = new Random(seed);
    }

    public PaymentRequest next() {
        return new PaymentRequest(
                "cust-" + random.nextInt(5000),
                "acc-" + random.nextInt(5000),
                "merch-" + random.nextInt(500),
                amount(),
                "ZAR",
                pick(CHANNELS),
                pick(TYPES),
                random.nextDouble() < 0.10 ? null : "device-" + random.nextInt(8000),
                random.nextDouble() < 0.10 ? pick(FOREIGN_COUNTRIES) : "ZA",
                outcome());
    }

    /** Mostly everyday amounts, with occasional large ones so all fraud levels appear. */
    private long amount() {
        double roll = random.nextDouble();
        if (roll < 0.90) {
            return 1_000 + random.nextInt(500_000);
        }
        if (roll < 0.98) {
            return 1_000_000 + random.nextInt(2_000_000);
        }
        return 5_000_000 + random.nextInt(5_000_000);
    }

    private Outcome outcome() {
        double roll = random.nextDouble();
        if (roll < 0.08) {
            return Outcome.NOT_AUTHORISED;
        }
        if (roll < 0.13) {
            return Outcome.INVALIDATED;
        }
        if (roll < 0.17) {
            return Outcome.NOT_COMPLETED;
        }
        return Outcome.COMPLETED;
    }

    private String pick(List<String> values) {
        return values.get(random.nextInt(values.size()));
    }
}
