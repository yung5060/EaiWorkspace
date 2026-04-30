package com.yung.cho.eaigateway.config;

// 리우팅 정보 vo
public record ServiceRouteConfig (
        String uri,     // 타겟지 url
        long timeoutMs,     // 타겟지 타임아웃 ms
        float failureRateThreshold,     // 지난 10초간 실패율
        int concurrentSessions      // 동접 세션수
) {}

