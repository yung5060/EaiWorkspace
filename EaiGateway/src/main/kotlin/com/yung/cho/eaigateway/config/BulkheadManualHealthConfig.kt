package com.yung.cho.eaigateway.config

import io.github.resilience4j.bulkhead.BulkheadRegistry
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class BulkheadManualHealthConfig {

    @Bean
    fun bulkhead(bulkheadRegistry: BulkheadRegistry): HealthIndicator {
        return HealthIndicator {
            val details = mutableMapOf<String, Any>()

            // Collect stats for all active bulkheads
            bulkheadRegistry.allBulkheads.forEach { bh ->
                val bhStats = mutableMapOf<String, Any>()
                bhStats["availableCalls"] = bh.metrics.availableConcurrentCalls
                bhStats["maxCalls"] = bh.bulkheadConfig.maxConcurrentCalls
                details[bh.name] = bhStats
            }

            Health.up()
                .withDetails(details)
                .build()
        }
    }
}
