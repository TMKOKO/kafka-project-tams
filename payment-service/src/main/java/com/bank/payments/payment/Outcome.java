package com.bank.payments.payment;

import com.bank.payments.common.PaymentEventType;

import java.util.List;

import static com.bank.payments.common.PaymentEventType.*;

/** How a journey ends, and the events it emits in order. Follows the state flow in the brief. */
public enum Outcome {
    COMPLETED(null, PAYMENT_INITIATED, PAYMENT_AUTHORISED, PAYMENT_VALIDATED, PAYMENT_COMPLETED),
    NOT_AUTHORISED("INSUFFICIENT_FUNDS", PAYMENT_INITIATED, PAYMENT_NOT_AUTHORISED),
    INVALIDATED("INVALID_BENEFICIARY", PAYMENT_INITIATED, PAYMENT_AUTHORISED, PAYMENT_INVALIDATED),
    NOT_COMPLETED("RAIL_TIMEOUT", PAYMENT_INITIATED, PAYMENT_AUTHORISED, PAYMENT_VALIDATED, PAYMENT_NOT_COMPLETED);

    private final String failureReason;
    private final List<PaymentEventType> stages;

    Outcome(String failureReason, PaymentEventType... stages) {
        this.failureReason = failureReason;
        this.stages = List.of(stages);
    }

    public List<PaymentEventType> stages() {
        return stages;
    }

    /** The reason code applies only to the failing stage. */
    public String reasonFor(PaymentEventType stage) {
        return stage.isFailure() ? failureReason : null;
    }
}
