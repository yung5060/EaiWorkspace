package com.yung.cho.eaigateway.config

import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.cloud.autoconfigure.RefreshAutoConfiguration
import org.springframework.core.env.MapPropertySource

class RoutingConfigTest {

    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(RoutingConfig::class.java, RefreshAutoConfiguration::class.java))
        .withBean(BulkheadRegistry::class.java, { BulkheadRegistry.ofDefaults() })
        .withBean(CircuitBreakerRegistry::class.java, { CircuitBreakerRegistry.ofDefaults() })
        .withBean(TimeLimiterRegistry::class.java, { TimeLimiterRegistry.ofDefaults() })

    @Test
    fun `routingMap WithRouteInfoPropertySource LoadsCorrectly`() {
        contextRunner.withInitializer { context ->
            val map = mutableMapOf<String, Any>()
            map["route1"] = "http://localhost:8081, 5000, 50.0, 100"
            map["route2"] = "http://localhost:8082"
            context.environment.propertySources.addFirst(MapPropertySource("my-routeInfo", map))
        }.run { context ->
            assertThat(context).hasBean("routingMap")
            @Suppress("UNCHECKED_CAST")
            val routingMap = context.getBean("routingMap") as Map<String, ServiceRouteConfig>
            
            assertThat(routingMap).containsKey("route1")
            val config1 = routingMap["route1"]!!
            assertThat(config1.uri).isEqualTo("http://localhost:8081")
            assertThat(config1.timeoutMs).isEqualTo(5000L)
            assertThat(config1.failureRateThreshold).isEqualTo(50.0f)
            assertThat(config1.concurrentSessions).isEqualTo(100)

            assertThat(routingMap).containsKey("route2")
            val config2 = routingMap["route2"]!!
            assertThat(config2.uri).isEqualTo("http://localhost:8082")
            assertThat(config2.timeoutMs).isEqualTo(70000L) // Default value
            assertThat(config2.failureRateThreshold).isEqualTo(50.0f) // Default value
            assertThat(config2.concurrentSessions).isEqualTo(50) // Default value
        }
    }

    @Test
    fun `routingMap InitializesRegistries`() {
        contextRunner.withInitializer { context ->
            val map = mutableMapOf<String, Any>()
            map["route3"] = "http://localhost:8083, 3000, 25.0, 10"
            context.environment.propertySources.addFirst(MapPropertySource("test-routeInfo", map))
        }.run { context ->
            val cbRegistry = context.getBean(CircuitBreakerRegistry::class.java)
            assertThat(cbRegistry.find("route3")).isPresent
            assertThat(cbRegistry.circuitBreaker("route3").circuitBreakerConfig.failureRateThreshold).isEqualTo(25.0f)

            val bulkheadRegistry = context.getBean(BulkheadRegistry::class.java)
            assertThat(bulkheadRegistry.find("route3")).isPresent
            assertThat(bulkheadRegistry.bulkhead("route3").bulkheadConfig.maxConcurrentCalls).isEqualTo(10)
            
            val tlRegistry = context.getBean(TimeLimiterRegistry::class.java)
            assertThat(tlRegistry.find("route3")).isPresent
            assertThat(tlRegistry.timeLimiter("route3").timeLimiterConfig.timeoutDuration.toMillis()).isEqualTo(3000L)
        }
    }
}
