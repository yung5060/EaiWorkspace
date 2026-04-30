package com.yung.cho.eaigateway.logging;

import java.time.ZonedDateTime;

// 카프카 pub용 로그 dto
public record GatewayLogEvent(
        ZonedDateTime timestamp
        , String phase
        , String requestUri
        , String body
) {
}