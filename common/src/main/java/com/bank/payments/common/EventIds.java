package com.bank.payments.common;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public final class EventIds {
    private EventIds() {
    }

    /**
     * Derives a downstream event id from its source event, so reprocessing the same source after a
     * rebalance or retry yields the same id and downstream systems can recognise the duplicate.
     */
    public static String derive(String sourceEventId, String kind) {
        return UUID.nameUUIDFromBytes((kind + ":" + sourceEventId).getBytes(StandardCharsets.UTF_8)).toString();
    }
}
