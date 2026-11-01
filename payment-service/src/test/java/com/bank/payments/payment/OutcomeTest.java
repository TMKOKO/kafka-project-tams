package com.bank.payments.payment;

import com.bank.payments.common.PaymentEventType;
import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static com.bank.payments.common.PaymentEventType.*;
import static org.assertj.core.api.Assertions.assertThat;

class OutcomeTest {

    @Test
    void stagesFollowTheFlowInTheBrief() {
        assertThat(Outcome.COMPLETED.stages())
                .containsExactly(PAYMENT_INITIATED, PAYMENT_AUTHORISED, PAYMENT_VALIDATED, PAYMENT_COMPLETED);
        assertThat(Outcome.NOT_AUTHORISED.stages()).containsExactly(PAYMENT_INITIATED, PAYMENT_NOT_AUTHORISED);
        assertThat(Outcome.INVALIDATED.stages())
                .containsExactly(PAYMENT_INITIATED, PAYMENT_AUTHORISED, PAYMENT_INVALIDATED);
        assertThat(Outcome.NOT_COMPLETED.stages())
                .containsExactly(PAYMENT_INITIATED, PAYMENT_AUTHORISED, PAYMENT_VALIDATED, PAYMENT_NOT_COMPLETED);
    }

    @Test
    void reasonCodeAppearsOnlyOnTheFailingStage() {
        List<PaymentEventType> stages = Outcome.NOT_COMPLETED.stages();
        assertThat(Outcome.NOT_COMPLETED.reasonFor(stages.get(0))).isNull();
        assertThat(Outcome.NOT_COMPLETED.reasonFor(PAYMENT_NOT_COMPLETED)).isEqualTo("RAIL_TIMEOUT");
        assertThat(Outcome.COMPLETED.reasonFor(PAYMENT_COMPLETED)).isNull();
    }

    @Test
    void generatorIsRepeatableAndProducesEveryOutcome() {
        RequestGenerator first = new RequestGenerator(42);
        RequestGenerator second = new RequestGenerator(42);
        Map<Outcome, Integer> counts = new EnumMap<>(Outcome.class);
        for (int i = 0; i < 1000; i++) {
            PaymentRequest request = first.next();
            assertThat(request).isEqualTo(second.next());
            counts.merge(request.outcome(), 1, Integer::sum);
        }
        assertThat(counts).containsOnlyKeys(Outcome.values());
    }
}
