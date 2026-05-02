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

    // 라우팅 정보 캐싱
    @RefreshScope
    @Bean(name = "routingMap")
    public Map<String, ServiceRouteConfig> routingMap(
            ConfigurableEnvironment env
            , BulkheadRegistry bulkheadRegistry
            , CircuitBreakerRegistry circuitBreakerRegistry
            , TimeLimiterRegistry timeLimiterRegistry
    ) {
        Map<String, ServiceRouteConfig> map = new LinkedHashMap<>();
        // 모든 라우팅 정보를 프로퍼티에서 Map으로 캐싱하는 loop
        for (PropertySource<?> ps : env.getPropertySources()) {
            if (ps.getName().contains("routeInfo") && ps instanceof EnumerablePropertySource<?> eps) {      // -routeInfo 컨피그만 읽도록
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

        // 인스턴스 기동 또는 actuator refresh 될때 모든 라우팅 옵션 초기화(런타임 배포)
        map.forEach((key, config) -> {
            CircuitBreakerConfig cbConfig = CircuitBreakerConfig.custom()       // 서킷브레이커 옵션 (재)조정
                    .failureRateThreshold(config.failureRateThreshold())
                    .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.TIME_BASED)
                    .slidingWindowSize(60)
                    .minimumNumberOfCalls(10)
                    .waitDurationInOpenState(Duration.ofSeconds(10))
                    .build();
            TimeLimiterConfig tlConfig = TimeLimiterConfig.custom()             // 타임아웃 옵션 (재)조정
                    .timeoutDuration(Duration.ofMillis(config.timeoutMs()))
                    .build();

            // 기존 서킷브레이커 + 타임아웃 옵션 제거
            circuitBreakerRegistry.remove(key);
            timeLimiterRegistry.remove(key);
            // 신규 서킷브레이커 + 옵션 적용
            circuitBreakerRegistry.circuitBreaker(key, cbConfig);
            timeLimiterRegistry.timeLimiter(key, tlConfig);

            // 세션제어 옵션 조정 및 런타임 배포
            BulkheadConfig newBulkheadConfig = BulkheadConfig.custom()
                    .maxConcurrentCalls(config.concurrentSessions())
                    .maxWaitDuration(Duration.ofMillis(10))
                    .build();
            bulkheadRegistry.find(key).ifPresentOrElse(
                    existingBulkhead -> existingBulkhead.changeConfig(newBulkheadConfig)        // 세션제어 옵션 재조정(변경)
                    , () -> bulkheadRegistry.bulkhead(key, newBulkheadConfig)                   // 기존 옵션 없을시 새로 조정
            );
        });

        return map;
    }
}
