package com.bank.payments.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.List;

/** Value of a record on fraud.scores.v1. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FraudEvent(
        String eventId,
        int schemaVersion,
        Type eventType,
        String paymentId,
        String customerId,
        int score,
        List<String> reasons,
        Instant occurredAt,
        String correlationId,
        String causationId) {

    public enum Type {
        FRAUD_SCORE_LOW,
        FRAUD_SCORE_MEDIUM,
        FRAUD_SCORE_HIGH
    }
}
