package com.yung.cho.eaigateway.config;

import io.github.resilience4j.bulkhead.BulkheadRegistry;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class BulkheadManualHealthConfig {       // Bulkhead(세션매니저) metric 정보 /actuator/health 노출

    @Bean
    public HealthIndicator bulkhead(BulkheadRegistry bulkheadRegistry) {
        return () -> {
            Map<String, Object> details = new HashMap<>();

            // Collect stats for all active bulkheads
            bulkheadRegistry.getAllBulkheads().forEach(bh -> {
                Map<String, Object> bhStats = new HashMap<>();
                bhStats.put("availableCalls", bh.getMetrics().getAvailableConcurrentCalls());
                bhStats.put("maxCalls", bh.getBulkheadConfig().getMaxConcurrentCalls());
                details.put(bh.getName(), bhStats);
            });

            return Health.up()
                    .withDetails(details)
                    .build();
        };
    }
}