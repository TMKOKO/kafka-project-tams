package com.bank.payments.common;

public enum PaymentEventType {
    PAYMENT_INITIATED,
    PAYMENT_AUTHORISED,
    PAYMENT_NOT_AUTHORISED,
    PAYMENT_VALIDATED,
    PAYMENT_INVALIDATED,
    PAYMENT_COMPLETED,
    PAYMENT_NOT_COMPLETED;

    /** The brief's "payment failed": any stage that ended the journey unsuccessfully. */
    public boolean isFailure() {
        return this == PAYMENT_NOT_AUTHORISED || this == PAYMENT_INVALIDATED || this == PAYMENT_NOT_COMPLETED;
    }

    public boolean isTerminal() {
        return isFailure() || this == PAYMENT_COMPLETED;
    }
}
