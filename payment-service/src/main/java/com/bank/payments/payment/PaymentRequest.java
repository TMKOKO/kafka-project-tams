package com.bank.payments.payment;

/** The input to a journey. A null deviceId means an unrecognised device, which fraud scoring penalises. */
public record PaymentRequest(
        String customerId,
        String accountId,
        String merchantId,
        long amountMinor,
        String currency,
        String channel,
        String paymentType,
        String deviceId,
        String ipCountry,
        Outcome outcome) {
}
