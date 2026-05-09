package com.yung.cho.eaigateway.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.yung.cho.eaigateway.logging.GatewayLogProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CommonReplyFilterTest {

    private CommonReplyFilter filter;
    private ObjectMapper objectMapper;

    @Mock
    private GatewayLogProducer gatewayLogProducer;
    @Mock
    private GatewayFilterChain chain;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        filter = new CommonReplyFilter(gatewayLogProducer, objectMapper);
    }

    @Test
    void filter_SuccessfulResponse_LogsToKafka() {
        // Given
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/test").build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);
        exchange.getAttributes().put(ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR, URI.create("http://backend/api/test"));

        when(chain.filter(any())).thenAnswer(invocation -> {
            ServerWebExchange ex = invocation.getArgument(0);
            DataBuffer buffer = new DefaultDataBufferFactory().wrap("response body".getBytes(StandardCharsets.UTF_8));
            return ex.getResponse().writeWith(Flux.just(buffer));
        });

        // When & Then
        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        // Verify Kafka logging was called (it's async so we might need a small timeout or just verify)
        verify(gatewayLogProducer, timeout(1000)).send(eq("gateway-logs"), any());
    }

    @Test
    void filter_ChainError_LogsToKafkaAndReturnsBadGateway() {
        // Given
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/test").build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);
        exchange.getAttributes().put(ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR, URI.create("http://backend/api/test"));

        when(chain.filter(any())).thenReturn(Mono.error(new RuntimeException("Test Error")));

        // When & Then
        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        assertEquals(HttpStatus.BAD_GATEWAY, exchange.getResponse().getStatusCode());
        verify(gatewayLogProducer, timeout(1000)).send(eq("gateway-logs"), any());
    }
}
