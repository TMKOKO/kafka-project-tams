package com.bank.payments.notification;

import com.bank.payments.common.NotificationEvent;
import com.bank.payments.common.PaymentEventType;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NotificationTest {

    @Test
    void validatedMeansApproved() {
        assertThat(NotificationMapper.typeFor(PaymentEventType.PAYMENT_VALIDATED))
                .contains(NotificationEvent.Type.NOTIFICATION_PAYMENT_APPROVED);
    }

    @Test
    void everyFailureMeansFailed() {
        for (PaymentEventType type : new PaymentEventType[]{PaymentEventType.PAYMENT_NOT_AUTHORISED,
                PaymentEventType.PAYMENT_INVALIDATED, PaymentEventType.PAYMENT_NOT_COMPLETED}) {
            assertThat(NotificationMapper.typeFor(type)).contains(NotificationEvent.Type.NOTIFICATION_PAYMENT_FAILED);
        }
    }

    @Test
    void otherEventsDoNotNotify() {
        for (PaymentEventType type : new PaymentEventType[]{PaymentEventType.PAYMENT_INITIATED,
                PaymentEventType.PAYMENT_AUTHORISED, PaymentEventType.PAYMENT_COMPLETED}) {
            assertThat(NotificationMapper.typeFor(type)).isEmpty();
        }
    }

    @Test
    void senderRetriesThenSucceeds() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        // Fails while the roll is below the failure rate: first attempt fails, second succeeds.
        MockSender sender = new MockSender(0.5, () -> calls.incrementAndGet() == 1 ? 0.0 : 0.9);
        sender.send("c", "p", "m");
        assertThat(calls).hasValue(2);
    }

    @Test
    void senderThrowsOnceAttemptsRunOutSoTheEventCanGoToTheDlq() {
        AtomicInteger calls = new AtomicInteger();
        MockSender sender = new MockSender(1.0, () -> {
            calls.incrementAndGet();
            return 0.0;
        });
        assertThatThrownBy(() -> sender.send("c", "p", "m")).isInstanceOf(MockSender.GatewayException.class);
        assertThat(calls).hasValue(MockSender.MAX_ATTEMPTS);
    }
}
