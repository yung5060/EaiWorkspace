package com.yung.cho.eaigateway.logging

import com.fasterxml.jackson.databind.annotation.JsonSerialize
import java.time.ZonedDateTime

/**
 * 카프카 pub용 로그 dto
 */
data class GatewayLogEvent(
    val timestamp: ZonedDateTime,
    val phase: String,
    val requestUri: String,
    @JsonSerialize(using = ByteArrayToStringSerializer::class)
    val body: ByteArray
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as GatewayLogEvent

        if (timestamp != other.timestamp) return false
        if (phase != other.phase) return false
        if (requestUri != other.requestUri) return false
        if (!body.contentEquals(other.body)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = timestamp.hashCode()
        result = 31 * result + phase.hashCode()
        result = 31 * result + requestUri.hashCode()
        result = 31 * result + body.contentHashCode()
        return result
    }
}
