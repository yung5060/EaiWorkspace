package com.yung.cho.eaigateway.config

import io.micrometer.core.instrument.binder.MeterBinder
import io.micrometer.core.instrument.binder.netty4.NettyAllocatorMetrics
import io.netty.buffer.PooledByteBufAllocator
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * Exposes Netty's direct memory allocator metrics to Prometheus.
 * This is crucial for monitoring off-heap memory usage in Spring Cloud Gateway.
 */
@Configuration
class NettyMetricsConfig {

    @Bean
    fun nettyAllocatorMetrics(): MeterBinder {
        // Monitors the pooled direct memory allocator used by Netty/Reactor Netty
        return NettyAllocatorMetrics(PooledByteBufAllocator.DEFAULT)
    }
}
