package com.bank.payments.fraud;

import com.bank.payments.common.FraudEvent;
import com.bank.payments.common.PaymentEvent;
import com.bank.payments.common.PaymentEventType;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class FraudScorerTest {
    private final FraudScorer scorer = new FraudScorer();

    private static PaymentEvent event(long amountMinor, String deviceId, String country) {
        return new PaymentEvent("e", 1, PaymentEventType.PAYMENT_INITIATED, "p", "c", "a", "m", amountMinor, "ZAR",
                "WEB", "CARD", deviceId, country, null, Instant.now(), "corr");
    }

    @Test
    void everydayPaymentIsLow() {
        FraudScorer.Result result = scorer.score(event(25_000, "device-1", "ZA"));
        assertThat(result.score()).isZero();
        assertThat(result.type()).isEqualTo(FraudEvent.Type.FRAUD_SCORE_LOW);
    }

    @Test
    void largeAmountAloneStaysLowJustUnderTheMediumThreshold() {
        FraudScorer.Result result = scorer.score(event(1_000_000, "device-1", "ZA"));
        assertThat(result.score()).isEqualTo(25);
        assertThat(result.type()).isEqualTo(FraudEvent.Type.FRAUD_SCORE_LOW);
    }

    @Test
    void largeCrossBorderPaymentIsMedium() {
        FraudScorer.Result result = scorer.score(event(1_000_000, "device-1", "GB"));
        assertThat(result.score()).isEqualTo(45);
        assertThat(result.type()).isEqualTo(FraudEvent.Type.FRAUD_SCORE_MEDIUM);
        assertThat(result.reasons()).containsExactly("LARGE_AMOUNT", "CROSS_BORDER");
    }

    @Test
    void veryLargePaymentFromHighRiskCountryOnUnknownDeviceIsHigh() {
        FraudScorer.Result result = scorer.score(event(7_500_000, null, "RU"));
        assertThat(result.score()).isEqualTo(95);
        assertThat(result.type()).isEqualTo(FraudEvent.Type.FRAUD_SCORE_HIGH);
    }

    @Test
    void scoreOfExactlySixtyIsHigh() {
        FraudScorer.Result result = scorer.score(event(1_000_000, null, "GB"));
        assertThat(result.score()).isEqualTo(60);
        assertThat(result.type()).isEqualTo(FraudEvent.Type.FRAUD_SCORE_HIGH);
    }
}
