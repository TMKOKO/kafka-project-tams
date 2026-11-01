package com.bank.payments.audit;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
public class AuditController {
    private final AuditLog auditLog;

    public AuditController(AuditLog auditLog) {
        this.auditLog = auditLog;
    }

    @GetMapping("/audit/verify")
    public Map<String, Object> verify() {
        Map<String, Object> report = new LinkedHashMap<>(auditLog.verifier().verify());
        report.put("fraudEvents", auditLog.fraudLevels());
        return report;
    }

    @GetMapping("/audit/payments/{paymentId}")
    public List<OrderingVerifier.Entry> timeline(@PathVariable String paymentId) {
        return auditLog.timeline(paymentId);
    }
}
