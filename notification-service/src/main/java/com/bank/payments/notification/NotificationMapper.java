package com.bank.payments.notification;

import com.bank.payments.common.NotificationEvent;
import com.bank.payments.common.PaymentEventType;

import java.util.Optional;

/** Decides which payment events notify the customer. VALIDATED means approved; any failure means failed. */
public final class NotificationMapper {
    private NotificationMapper() {
    }

    public static Optional<NotificationEvent.Type> typeFor(PaymentEventType eventType) {
        if (eventType == PaymentEventType.PAYMENT_VALIDATED) {
            return Optional.of(NotificationEvent.Type.NOTIFICATION_PAYMENT_APPROVED);
        }
        return eventType.isFailure() ? Optional.of(NotificationEvent.Type.NOTIFICATION_PAYMENT_FAILED) : Optional.empty();
    }

    public static String messageFor(NotificationEvent.Type type, String reasonCode) {
        return type == NotificationEvent.Type.NOTIFICATION_PAYMENT_APPROVED
                ? "Your payment was approved."
                : "Your payment failed (" + reasonCode + ").";
    }
}
