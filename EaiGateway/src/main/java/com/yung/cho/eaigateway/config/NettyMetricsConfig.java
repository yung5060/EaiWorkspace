package com.yung.cho.eaigateway.config;

import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.binder.netty4.NettyAllocatorMetrics;
import io.netty.buffer.PooledByteBufAllocator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Exposes Netty's direct memory allocator metrics to Prometheus.
 * This is crucial for monitoring off-heap memory usage in Spring Cloud Gateway.
 */
@Configuration
public class NettyMetricsConfig {

    @Bean
    public MeterBinder nettyAllocatorMetrics() {
        // Monitors the pooled direct memory allocator used by Netty/Reactor Netty
        return new NettyAllocatorMetrics(PooledByteBufAllocator.DEFAULT);
    }
}
