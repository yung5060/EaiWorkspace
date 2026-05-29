package com.yung.cho.eaigateway.config

/**
 * 라우팅 정보 vo
 */
data class ServiceRouteConfig(
    val uri: String,                // 타겟지 url
    val timeoutMs: Long,            // 타겟지 타임아웃 ms
    val failureRateThreshold: Float, // 지난 10초간 실패율
    val concurrentSessions: Int     // 동접 세션수
)
