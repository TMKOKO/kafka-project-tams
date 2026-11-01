package com.bank.payments.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;

/** Value of a record on payments.events.v1. Unknown fields are ignored so the schema can grow. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PaymentEvent(
        String eventId,
        int schemaVersion,
        PaymentEventType eventType,
        String paymentId,
        String customerId,
        String accountId,
        String merchantId,
        long amountMinor,
        String currency,
        String channel,
        String paymentType,
        String deviceId,
        String ipCountry,
        String reasonCode,
        Instant occurredAt,
        String correlationId) {

    public static final int SCHEMA_VERSION = 1;

    /** Valid JSON can still be an unusable event, for example {@code {}}. Such records go to the DLQ. */
    public PaymentEvent requireValid() {
        if (eventId == null || eventType == null || paymentId == null || occurredAt == null) {
            throw new IllegalArgumentException("Missing eventId, eventType, paymentId or occurredAt");
        }
        return this;
    }
}
