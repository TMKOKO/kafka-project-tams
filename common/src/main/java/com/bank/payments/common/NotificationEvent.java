package com.bank.payments.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;

/** Value of a record on notifications.events.v1. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record NotificationEvent(
        String eventId,
        int schemaVersion,
        Type eventType,
        String paymentId,
        String customerId,
        String deliveryChannel,
        String message,
        Instant occurredAt,
        String correlationId,
        String causationId) {

    public enum Type {
        NOTIFICATION_PAYMENT_APPROVED,
        NOTIFICATION_PAYMENT_FAILED
    }
}
