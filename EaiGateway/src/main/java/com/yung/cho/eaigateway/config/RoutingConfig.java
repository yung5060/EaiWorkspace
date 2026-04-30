package com.yung.cho.eaigateway.config;

import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.PropertySource;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

@Configuration
public class RoutingConfig {

    @RefreshScope
    @Bean(name = "routingMap")
    public Map<String, ServiceRouteConfig> routingMap(
            ConfigurableEnvironment env
            , BulkheadRegistry bulkheadRegistry
            , CircuitBreakerRegistry circuitBreakerRegistry
            , TimeLimiterRegistry timeLimiterRegistry
    ) {
        Map<String, ServiceRouteConfig> map = new LinkedHashMap<>();
        for (PropertySource<?> ps : env.getPropertySources()) {
            if (ps.getName().contains("routeInfo") && ps instanceof EnumerablePropertySource<?> eps) {
                for (String name : eps.getPropertyNames()) {
                    Object rawValue = eps.getProperty(name);
                    String[] parts = rawValue.toString().split(",");

                    String uri = parts[0].trim();
                    long timeout = parts.length > 1 && !parts[1].trim().isBlank() ? Long.parseLong(parts[1].trim()) : 70000L;
                    float failureRate = parts.length > 2 && !parts[2].trim().isBlank() ? Float.parseFloat(parts[2].trim()) : 50.0f;
                    int concurrentSessions = parts.length > 3 && !parts[3].trim().isBlank() ? Integer.parseInt(parts[3].trim()) : 50;

                    ServiceRouteConfig config = new ServiceRouteConfig(uri, timeout, failureRate, concurrentSessions);

                    map.put(name, config);
                }
            }
        }

        map.forEach((key, config) -> {
            CircuitBreakerConfig cbConfig = CircuitBreakerConfig.custom()
                    .failureRateThreshold(config.failureRateThreshold())
                    .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                    .slidingWindowSize(10)
                    .waitDurationInOpenState(Duration.ofSeconds(10))
                    .build();
            TimeLimiterConfig tlConfig = TimeLimiterConfig.custom()
                    .timeoutDuration(Duration.ofMillis(config.timeoutMs()))
                    .build();

            circuitBreakerRegistry.remove(key);
            timeLimiterRegistry.remove(key);

            circuitBreakerRegistry.circuitBreaker(key, cbConfig);
            timeLimiterRegistry.timeLimiter(key, tlConfig);

            BulkheadConfig newBulkheadConfig = BulkheadConfig.custom()
                    .maxConcurrentCalls(config.concurrentSessions())
                    .maxWaitDuration(Duration.ofMillis(10))
                    .build();
            bulkheadRegistry.find(key).ifPresentOrElse(
                    existingBulkhead -> existingBulkhead.changeConfig(newBulkheadConfig)
                    , () -> bulkheadRegistry.bulkhead(key, newBulkheadConfig)
            );
        });

        return map;
    }
}
