package com.yung.cho.eaigateway.logging

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.charset.StandardCharsets
import java.time.ZonedDateTime

class GatewayLogEventTest {

    @Test
    fun `serialization WorksWithCustomSerializer`() {
        val mapper = ObjectMapper()
            .registerModule(JavaTimeModule())
            .registerKotlinModule()
            
        val body = "{\"foo\":\"bar\"}".toByteArray(StandardCharsets.UTF_8)
        val event = GatewayLogEvent(ZonedDateTime.now(), "[REQ1]", "http://test", body)
        
        val jsonBytes = mapper.writeValueAsBytes(event)
        val json = String(jsonBytes, StandardCharsets.UTF_8)
        
        // The custom serializer should pipe raw UTF-8 bytes into the JSON stream as a string
        assertThat(json).contains("\"body\":\"{\\\"foo\\\":\\\"bar\\\"}\"")
        assertThat(json).contains("\"phase\":\"[REQ1]\"")
        assertThat(json).contains("\"requestUri\":\"http://test\"")
    }
}
