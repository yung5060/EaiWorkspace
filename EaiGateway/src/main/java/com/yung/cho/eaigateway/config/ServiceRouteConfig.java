package com.yung.cho.eaigateway.config;

public record ServiceRouteConfig (
        String uri,
        long timeoutMs,
        float failureRateThreshold,
        int concurrentSessions
) {}

