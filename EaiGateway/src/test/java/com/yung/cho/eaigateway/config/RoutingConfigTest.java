package com.yung.cho.eaigateway.config;

import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.autoconfigure.RefreshAutoConfiguration;
import org.springframework.core.env.MapPropertySource;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RoutingConfigTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(RoutingConfig.class, RefreshAutoConfiguration.class))
            .withBean(BulkheadRegistry.class, BulkheadRegistry::ofDefaults)
            .withBean(CircuitBreakerRegistry.class, CircuitBreakerRegistry::ofDefaults)
            .withBean(TimeLimiterRegistry.class, TimeLimiterRegistry::ofDefaults);

    @Test
    void routingMap_WithRouteInfoPropertySource_LoadsCorrectly() {
        contextRunner.withInitializer(context -> {
            Map<String, Object> map = new HashMap<>();
            map.put("route1", "http://localhost:8081, 5000, 50.0, 100");
            map.put("route2", "http://localhost:8082");
            context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("my-routeInfo", map));
        }).run(context -> {
            assertThat(context).hasBean("routingMap");
            @SuppressWarnings("unchecked")
            Map<String, ServiceRouteConfig> routingMap = (Map<String, ServiceRouteConfig>) context.getBean("routingMap");
            
            assertThat(routingMap).containsKey("route1");
            ServiceRouteConfig config1 = routingMap.get("route1");
            assertThat(config1.uri()).isEqualTo("http://localhost:8081");
            assertThat(config1.timeoutMs()).isEqualTo(5000L);
            assertThat(config1.failureRateThreshold()).isEqualTo(50.0f);
            assertThat(config1.concurrentSessions()).isEqualTo(100);

            assertThat(routingMap).containsKey("route2");
            ServiceRouteConfig config2 = routingMap.get("route2");
            assertThat(config2.uri()).isEqualTo("http://localhost:8082");
            assertThat(config2.timeoutMs()).isEqualTo(70000L); // Default value
            assertThat(config2.failureRateThreshold()).isEqualTo(50.0f); // Default value
            assertThat(config2.concurrentSessions()).isEqualTo(50); // Default value
        });
    }

    @Test
    void routingMap_InitializesRegistries() {
        contextRunner.withInitializer(context -> {
            Map<String, Object> map = new HashMap<>();
            map.put("route3", "http://localhost:8083, 3000, 25.0, 10");
            context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("test-routeInfo", map));
        }).run(context -> {
            CircuitBreakerRegistry cbRegistry = context.getBean(CircuitBreakerRegistry.class);
            assertThat(cbRegistry.find("route3")).isPresent();
            assertThat(cbRegistry.circuitBreaker("route3").getCircuitBreakerConfig().getFailureRateThreshold()).isEqualTo(25.0f);

            BulkheadRegistry bulkheadRegistry = context.getBean(BulkheadRegistry.class);
            assertThat(bulkheadRegistry.find("route3")).isPresent();
            assertThat(bulkheadRegistry.bulkhead("route3").getBulkheadConfig().getMaxConcurrentCalls()).isEqualTo(10);
            
            TimeLimiterRegistry tlRegistry = context.getBean(TimeLimiterRegistry.class);
            assertThat(tlRegistry.find("route3")).isPresent();
            assertThat(tlRegistry.timeLimiter("route3").getTimeLimiterConfig().getTimeoutDuration().toMillis()).isEqualTo(3000L);
        });
    }
}
