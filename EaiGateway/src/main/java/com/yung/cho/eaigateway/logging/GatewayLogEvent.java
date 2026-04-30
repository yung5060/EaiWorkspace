package com.yung.cho.eaigateway.logging;

import java.time.ZonedDateTime;

public record GatewayLogEvent(ZonedDateTime timestamp, String phase, String requestUri, String body) {

}