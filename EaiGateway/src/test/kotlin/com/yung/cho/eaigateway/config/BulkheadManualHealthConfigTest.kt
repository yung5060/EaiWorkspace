package com.yung.cho.eaigateway.config

import io.github.resilience4j.bulkhead.BulkheadRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.actuate.health.HealthIndicator

class BulkheadManualHealthConfigTest {

    @Test
    fun `bulkheadHealthIndicator ReturnsCorrectDetails`() {
        val registry = BulkheadRegistry.ofDefaults()
        registry.bulkhead("test-bh")
        
        val config = BulkheadManualHealthConfig()
        val indicator: HealthIndicator = config.bulkhead(registry)
        
        val health = indicator.health()
        
        assertThat(health.status.code).isEqualTo("UP")
        val details = health.details
        assertThat(details).containsKey("test-bh")
        
        @Suppress("UNCHECKED_CAST")
        val bhStats = details["test-bh"] as Map<String, Any>
        assertThat(bhStats).containsEntry("availableCalls", 25) // Default maxConcurrentCalls is 25
        assertThat(bhStats).containsEntry("maxCalls", 25)
    }
}
