package com.bank.payments.payment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
public class PaymentController {
    private static final Logger log = LoggerFactory.getLogger(PaymentController.class);

    private final JourneyService journeys;

    public PaymentController(JourneyService journeys) {
        this.journeys = journeys;
    }

    @PostMapping("/payments")
    public JourneyService.JourneyResult create(@RequestBody PaymentRequest request) {
        return journeys.run(request);
    }

    /** Four fixed journeys, one per possible ending, with increasing fraud risk. */
    @PostMapping("/scenarios")
    public List<JourneyService.JourneyResult> scenarios() {
        return List.of(
                journeys.run(new PaymentRequest("cust-1", "acc-1", "merch-1", 25_000, "ZAR", "MOBILE", "CARD",
                        "device-1", "ZA", Outcome.COMPLETED)),
                journeys.run(new PaymentRequest("cust-2", "acc-2", "merch-2", 1_500_000, "ZAR", "WEB", "EFT",
                        "device-2", "ZA", Outcome.NOT_AUTHORISED)),
                journeys.run(new PaymentRequest("cust-3", "acc-3", "merch-3", 2_000_000, "ZAR", "API", "INSTANT_EFT",
                        "device-3", "GB", Outcome.INVALIDATED)),
                journeys.run(new PaymentRequest("cust-4", "acc-4", "merch-4", 7_500_000, "ZAR", "WEB", "CARD",
                        null, "RU", Outcome.NOT_COMPLETED)));
    }

    /** Runs seeded journeys concurrently at a steady rate, so consumer lag reflects load and not a burst. */
    @PostMapping("/simulate")
    public Map<String, Object> simulate(@RequestParam(defaultValue = "1000") int count,
                                        @RequestParam(defaultValue = "42") long seed,
                                        @RequestParam(defaultValue = "100") int ratePerSec) throws Exception {
        RequestGenerator generator = new RequestGenerator(seed);
        Map<Outcome, AtomicInteger> outcomes = new EnumMap<>(Outcome.class);
        for (Outcome outcome : Outcome.values()) {
            outcomes.put(outcome, new AtomicInteger());
        }
        AtomicInteger events = new AtomicInteger();
        AtomicInteger unsent = new AtomicInteger();

        long startNanos = System.nanoTime();
        long intervalNanos = TimeUnit.SECONDS.toNanos(1) / ratePerSec;
        List<Future<?>> running = new ArrayList<>(count);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < count; i++) {
                long due = startNanos + i * intervalNanos;
                long waitNanos = due - System.nanoTime();
                if (waitNanos > 0) {
                    TimeUnit.NANOSECONDS.sleep(waitNanos);
                }
                PaymentRequest request = generator.next();
                running.add(executor.submit(() -> {
                    try {
                        JourneyService.JourneyResult result = journeys.run(request);
                        outcomes.get(result.outcome()).incrementAndGet();
                        events.addAndGet(result.events().size());
                    } catch (JourneyService.UnsentPaymentException e) {
                        unsent.incrementAndGet();
                    }
                }));
            }
        }
        for (Future<?> journey : running) {
            journey.get();
        }

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("journeysRequested", count);
        summary.put("seed", seed);
        summary.put("ratePerSec", ratePerSec);
        summary.put("eventsAcknowledged", events.get());
        summary.put("journeysUnsent", unsent.get());
        summary.put("outcomes", outcomes);
        summary.put("durationMs", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
        log.info("SIMULATION {}", summary);
        return summary;
    }

    @ExceptionHandler(JourneyService.UnsentPaymentException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public Map<String, String> unsent(JourneyService.UnsentPaymentException e) {
        return Map.of("error", e.getMessage());
    }
}
