package com.bank.payments.common;

/** Topic names and the header consumers use to filter without parsing the JSON value. */
public final class Topics {
    public static final String PAYMENTS = "payments.events.v1";
    public static final String FRAUD = "fraud.scores.v1";
    public static final String NOTIFICATIONS = "notifications.events.v1";

    public static final String HEADER_EVENT_TYPE = "event-type";

    private Topics() {
    }
}
