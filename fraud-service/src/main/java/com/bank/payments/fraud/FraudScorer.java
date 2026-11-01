package com.bank.payments.fraud;

import com.bank.payments.common.FraudEvent;
import com.bank.payments.common.PaymentEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Mock scoring: stateless rules, so a score needs nothing beyond the event and adds no I/O latency. */
public final class FraudScorer {
    static final String HOME_COUNTRY = "ZA";
    static final Set<String> HIGH_RISK_COUNTRIES = Set.of("RU", "NG");
    static final long LARGE_AMOUNT_MINOR = 1_000_000;
    static final long VERY_LARGE_AMOUNT_MINOR = 5_000_000;
    static final int MEDIUM_THRESHOLD = 30;
    static final int HIGH_THRESHOLD = 60;

    public record Result(int score, FraudEvent.Type type, List<String> reasons) {
    }

    public Result score(PaymentEvent event) {
        int score = 0;
        List<String> reasons = new ArrayList<>();

        if (event.amountMinor() >= VERY_LARGE_AMOUNT_MINOR) {
            score += 50;
            reasons.add("VERY_LARGE_AMOUNT");
        } else if (event.amountMinor() >= LARGE_AMOUNT_MINOR) {
            score += 25;
            reasons.add("LARGE_AMOUNT");
        }

        if (event.ipCountry() != null && HIGH_RISK_COUNTRIES.contains(event.ipCountry())) {
            score += 30;
            reasons.add("HIGH_RISK_COUNTRY");
        } else if (event.ipCountry() != null && !HOME_COUNTRY.equals(event.ipCountry())) {
            score += 20;
            reasons.add("CROSS_BORDER");
        }

        if (event.deviceId() == null) {
            score += 15;
            reasons.add("UNKNOWN_DEVICE");
        }

        return new Result(score, typeFor(score), reasons);
    }

    private static FraudEvent.Type typeFor(int score) {
        if (score >= HIGH_THRESHOLD) {
            return FraudEvent.Type.FRAUD_SCORE_HIGH;
        }
        return score >= MEDIUM_THRESHOLD ? FraudEvent.Type.FRAUD_SCORE_MEDIUM : FraudEvent.Type.FRAUD_SCORE_LOW;
    }
}
