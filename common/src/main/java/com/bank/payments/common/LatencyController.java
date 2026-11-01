package com.bank.payments.common;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class LatencyController {
    private final LatencyTracker tracker;

    public LatencyController(LatencyTracker tracker) {
        this.tracker = tracker;
    }

    @GetMapping("/latency")
    public Map<String, Map<String, Object>> latency() {
        return tracker.snapshot();
    }

    @PostMapping("/latency/reset")
    public void reset() {
        tracker.reset();
    }
}
