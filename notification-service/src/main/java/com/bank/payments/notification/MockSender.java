package com.bank.payments.notification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * Stands in for an SMS or push gateway. It fails at a configured rate so the retry path runs in the demo,
 * and throws once its attempts run out so the caller can send the event to the DLQ.
 */
@Component
public class MockSender {
    private static final Logger log = LoggerFactory.getLogger(MockSender.class);
    static final int MAX_ATTEMPTS = 3;
    private static final long BACKOFF_MS = 50;

    public static class GatewayException extends RuntimeException {
        public GatewayException(String message) {
            super(message);
        }
    }

    private final double failureRate;
    private final DoubleSupplier random;

    @org.springframework.beans.factory.annotation.Autowired
    public MockSender(@Value("${notification.mock.failure-rate:0.02}") double failureRate) {
        this(failureRate, () -> ThreadLocalRandom.current().nextDouble());
    }

    MockSender(double failureRate, DoubleSupplier random) {
        this.failureRate = failureRate;
        this.random = random;
    }

    public void send(String customerId, String paymentId, String message) throws InterruptedException {
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            if (random.getAsDouble() >= failureRate) {
                log.info("NOTIFIED customerId={} paymentId={} attempt={} message=\"{}\"", customerId, paymentId,
                        attempt, message);
                return;
            }
            log.warn("GATEWAY_FAILURE paymentId={} attempt={}/{}", paymentId, attempt, MAX_ATTEMPTS);
            if (attempt < MAX_ATTEMPTS) {
                Thread.sleep(BACKOFF_MS);
            }
        }
        throw new GatewayException("Gateway failed after " + MAX_ATTEMPTS + " attempts for payment " + paymentId);
    }
}
