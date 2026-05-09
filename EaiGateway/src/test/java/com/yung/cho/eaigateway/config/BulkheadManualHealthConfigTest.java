package com.yung.cho.eaigateway.config;

import io.github.resilience4j.bulkhead.BulkheadRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class BulkheadManualHealthConfigTest {

    @Test
    void bulkheadHealthIndicator_ReturnsCorrectDetails() {
        BulkheadRegistry registry = BulkheadRegistry.ofDefaults();
        registry.bulkhead("test-bh");
        
        BulkheadManualHealthConfig config = new BulkheadManualHealthConfig();
        HealthIndicator indicator = config.bulkhead(registry);
        
        Health health = indicator.health();
        
        assertThat(health.getStatus().getCode()).isEqualTo("UP");
        Map<String, Object> details = health.getDetails();
        assertThat(details).containsKey("test-bh");
        
        @SuppressWarnings("unchecked")
        Map<String, Object> bhStats = (Map<String, Object>) details.get("test-bh");
        assertThat(bhStats).containsEntry("availableCalls", 25); // Default maxConcurrentCalls is 25
        assertThat(bhStats).containsEntry("maxCalls", 25);
    }
}
