package com.yung.cho.eaigateway.config

import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.timelimiter.TimeLimiterConfig
import io.github.resilience4j.timelimiter.TimeLimiterRegistry
import org.springframework.cloud.context.config.annotation.RefreshScope
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.ConfigurableEnvironment
import org.springframework.core.env.EnumerablePropertySource
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

@Configuration
class RoutingConfig {

    // 라우팅 정보 캐싱
    @RefreshScope
    @Bean(name = ["routingMap"])
    fun routingMap(
        env: ConfigurableEnvironment,
        bulkheadRegistry: BulkheadRegistry,
        circuitBreakerRegistry: CircuitBreakerRegistry,
        timeLimiterRegistry: TimeLimiterRegistry
    ): Map<String, ServiceRouteConfig> {
        val map = ConcurrentHashMap<String, ServiceRouteConfig>()
        
        // 모든 라우팅 정보를 프로퍼티에서 Map으로 캐싱하는 loop
        for (ps in env.propertySources) {
            if (ps.name.contains("routeInfo") && ps is EnumerablePropertySource<*>) {      // -routeInfo 컨피그만 읽도록
                for (name in ps.propertyNames) {
                    val rawValue = ps.getProperty(name) ?: continue
                    val parts = rawValue.toString().split(",")

                    val uri = parts.getOrNull(0)?.trim().toString()
                    val timeout = parts.getOrNull(1)?.trim()?.takeIf { it.isNotBlank() }?.toLong() ?: 70000L
                    val failureRate = parts.getOrNull(2)?.trim()?.takeIf { it.isNotBlank() }?.toFloat() ?: 50.0f
                    val concurrentSessions = parts.getOrNull(3)?.trim()?.takeIf { it.isNotBlank() }?.toInt() ?: 50

                    val config = ServiceRouteConfig(uri, timeout, failureRate, concurrentSessions)
                    map[name] = config
                }
            }
        }

        // 인스턴스 기동 또는 actuator refresh 될때 모든 라우팅 옵션 초기화(런타임 배포)
        map.forEach { (key, config) ->
            val cbConfig = CircuitBreakerConfig.custom()       // 서킷브레이커 옵션 (재)조정
                .failureRateThreshold(config.failureRateThreshold)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.TIME_BASED)
                .slidingWindowSize(60)
                .minimumNumberOfCalls(10)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .build()
                
            val tlConfig = TimeLimiterConfig.custom()             // 타임아웃 옵션 (재)조정
                .timeoutDuration(Duration.ofMillis(config.timeoutMs))
                .build()

            val bulkheadConfig = BulkheadConfig.custom()            // 세션제어 옵션 조정
                .maxConcurrentCalls(config.concurrentSessions)
                .maxWaitDuration(Duration.ofMillis(10))
                .build()

            // 기존 서킷브레이커 + 타임아웃 옵션 제거
            circuitBreakerRegistry.remove(key)
            timeLimiterRegistry.remove(key)
            bulkheadRegistry.remove(key)
            
            // 신규 서킷브레이커 + 옵션 적용
            circuitBreakerRegistry.circuitBreaker(key, cbConfig)
            timeLimiterRegistry.timeLimiter(key, tlConfig)
            bulkheadRegistry.bulkhead(key, bulkheadConfig)
        }
        return map
    }
}