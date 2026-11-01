package com.bank.payments.audit;

import com.bank.payments.audit.OrderingVerifier.Entry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.bank.payments.common.PaymentEventType.*;
import static org.assertj.core.api.Assertions.assertThat;

class OrderingVerifierTest {

    @Test
    void completeJourneyHasNoViolations() {
        assertThat(OrderingVerifier.check("p", List.of(
                new Entry(PAYMENT_INITIATED, 7, 10),
                new Entry(PAYMENT_AUTHORISED, 7, 11),
                new Entry(PAYMENT_VALIDATED, 7, 15),
                new Entry(PAYMENT_COMPLETED, 7, 20)))).isEmpty();
    }

    @Test
    void deliveryOrderDoesNotMatterBecauseOffsetsDecide() {
        assertThat(OrderingVerifier.check("p", List.of(
                new Entry(PAYMENT_NOT_AUTHORISED, 7, 11),
                new Entry(PAYMENT_INITIATED, 7, 10)))).isEmpty();
    }

    @Test
    void multiplePartitionsAreAViolation() {
        assertThat(OrderingVerifier.check("p", List.of(
                new Entry(PAYMENT_INITIATED, 7, 10),
                new Entry(PAYMENT_AUTHORISED, 8, 11)))).singleElement().asString().contains("2 partitions");
    }

    @Test
    void wrongOffsetOrderIsAViolation() {
        assertThat(OrderingVerifier.check("p", List.of(
                new Entry(PAYMENT_AUTHORISED, 7, 10),
                new Entry(PAYMENT_INITIATED, 7, 11)))).isNotEmpty();
    }

    @Test
    void skippingAStageIsAViolation() {
        assertThat(OrderingVerifier.check("p", List.of(
                new Entry(PAYMENT_INITIATED, 7, 10),
                new Entry(PAYMENT_COMPLETED, 7, 11)))).singleElement().asString().contains("illegal transition");
    }

    @Test
    void eventAfterATerminalStateIsAViolation() {
        assertThat(OrderingVerifier.check("p", List.of(
                new Entry(PAYMENT_INITIATED, 7, 10),
                new Entry(PAYMENT_NOT_AUTHORISED, 7, 11),
                new Entry(PAYMENT_AUTHORISED, 7, 12)))).isNotEmpty();
    }

    @Test
    void reportCountsEndingsAndInProgressPayments() {
        OrderingVerifier verifier = new OrderingVerifier();
        verifier.add("done", new Entry(PAYMENT_INITIATED, 1, 1));
        verifier.add("done", new Entry(PAYMENT_NOT_AUTHORISED, 1, 2));
        verifier.add("open", new Entry(PAYMENT_INITIATED, 2, 1));

        Map<String, Object> report = verifier.verify();
        assertThat(report).containsEntry("payments", 2).containsEntry("events", 3)
                .containsEntry("inProgress", 1).containsEntry("violationCount", 0);
    }
}
