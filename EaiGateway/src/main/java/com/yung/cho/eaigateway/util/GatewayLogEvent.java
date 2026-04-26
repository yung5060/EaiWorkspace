package com.yung.cho.eaigateway.util;

import java.time.Instant;

public record GatewayLogEvent(Instant timestamp, String phase, String requestUri, String body) {

}