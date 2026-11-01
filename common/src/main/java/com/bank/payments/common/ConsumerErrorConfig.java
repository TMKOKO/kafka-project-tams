package com.bank.payments.common;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Bad records are sent to a DLQ inside the listeners, so an exception reaching the container means the
 * write path itself failed (for example, no ISR quorum). Retrying forever without committing is correct
 * then. Spring's default gives up after 9 retries and skips the batch, which would lose events.
 */
@Configuration
public class ConsumerErrorConfig {
    @Bean
    public DefaultErrorHandler retryUntilKafkaRecovers() {
        return new DefaultErrorHandler(new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}
