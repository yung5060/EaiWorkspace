package com.yung.cho.eaigateway.logging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class GatewayLogEventTest {

    @Test
    void serialization_WorksWithCustomSerializer() throws Exception {
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        byte[] body = "{\"foo\":\"bar\"}".getBytes(StandardCharsets.UTF_8);
        GatewayLogEvent event = new GatewayLogEvent(ZonedDateTime.now(), "[REQ1]", "http://test", body);
        
        byte[] jsonBytes = mapper.writeValueAsBytes(event);
        String json = new String(jsonBytes, StandardCharsets.UTF_8);
        
        // The custom serializer should pipe raw UTF-8 bytes into the JSON stream as a string
        assertThat(json).contains("\"body\":\"{\\\"foo\\\":\\\"bar\\\"}\"");
        assertThat(json).contains("\"phase\":\"[REQ1]\"");
        assertThat(json).contains("\"requestUri\":\"http://test\"");
    }
}
