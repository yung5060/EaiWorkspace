package com.yung.cho.eaigateway.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.yung.cho.eaigateway.config.ServiceRouteConfig;
import com.yung.cho.eaigateway.logging.GatewayLogProducer;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cloud.circuitbreaker.resilience4j.ReactiveResilience4JCircuitBreakerFactory;
import org.springframework.cloud.client.circuitbreaker.ReactiveCircuitBreaker;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.http.MediaType;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CommonRoutingFilterTest {

    private CommonRoutingFilter filter;
    private Map<String, ServiceRouteConfig> routingMap;
    private ObjectMapper objectMapper;

    @Mock
    private GatewayLogProducer gatewayLogProducer;
    @Mock
    private ReactiveResilience4JCircuitBreakerFactory circuitBreakerFactory;
    @Mock
    private BulkheadRegistry bulkheadRegistry;
    @Mock
    private GatewayFilterChain chain;
    @Mock
    private ReactiveCircuitBreaker reactiveCircuitBreaker;
    @Mock
    private Bulkhead bulkhead;

    @BeforeEach
    void setUp() {
        routingMap = new HashMap<>();
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        filter = new CommonRoutingFilter(routingMap, gatewayLogProducer, objectMapper, circuitBreakerFactory, bulkheadRegistry);

        lenient().when(circuitBreakerFactory.create(anyString())).thenReturn(reactiveCircuitBreaker);
        lenient().when(bulkheadRegistry.bulkhead(anyString())).thenReturn(bulkhead);
        
        // Mock the bulkhead config to avoid NPE in BulkheadOperator
        BulkheadConfig bulkheadConfig = BulkheadConfig.custom().build();
        lenient().when(bulkhead.getBulkheadConfig()).thenReturn(bulkheadConfig);
        lenient().when(bulkhead.tryAcquirePermission()).thenReturn(true);
        
        // Mock the circuit breaker run method to just execute the given Mono
        lenient().when(reactiveCircuitBreaker.run(any(Mono.class), any())).thenAnswer(invocation -> invocation.getArgument(0));
        
        lenient().when(chain.filter(any())).thenReturn(Mono.empty());
    }

    @Test
    void filter_RestPath_ExtractsKeyFromHeader() {
        // Given
        routingMap.put("TEST_KEY", new ServiceRouteConfig("http://localhost:8081", 1000, 50.0f, 10));
        MockServerHttpRequest request = MockServerHttpRequest.post("/REST/service")
                .header("key", "TEST_KEY")
                .body("some body content");
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        ArgumentCaptor<ServerWebExchange> exchangeCaptor = ArgumentCaptor.forClass(ServerWebExchange.class);
        when(chain.filter(exchangeCaptor.capture())).thenReturn(Mono.empty());

        // When & Then
        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        ServerWebExchange capturedExchange = exchangeCaptor.getValue();
        Object actualUri = capturedExchange.getAttribute(ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR);
        if (!"http://localhost:8081/service".equals(actualUri != null ? actualUri.toString() : "null")) {
            org.junit.jupiter.api.Assertions.fail("Expected http://localhost:8081/service but got " + actualUri);
        }
        verify(circuitBreakerFactory).create("TEST_KEY");
    }

    @Test
    void filter_JsonBody_ExtractsKeyFromJson() {
        // Given
        routingMap.put("JSON_KEY", new ServiceRouteConfig("http://localhost:8082", 1000, 50.0f, 10));
        String jsonBody = "{\"header_part\": {\"key\": \"JSON_KEY\"}}";
        MockServerHttpRequest request = MockServerHttpRequest.post("/api/data")
                .contentType(MediaType.APPLICATION_JSON)
                .body(jsonBody);
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        ArgumentCaptor<ServerWebExchange> exchangeCaptor = ArgumentCaptor.forClass(ServerWebExchange.class);
        when(chain.filter(exchangeCaptor.capture())).thenReturn(Mono.empty());

        // When & Then
        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        ServerWebExchange capturedExchange = exchangeCaptor.getValue();
        Object actualUri = capturedExchange.getAttribute(ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR);
        assertEquals("http://localhost:8082", actualUri != null ? actualUri.toString() : "null", "URI should match for JSON body");
        verify(circuitBreakerFactory).create("JSON_KEY");
    }

    @Test
    void filter_DefaultBody_ExtractsKeyFromPrefix() {
        // Given
        routingMap.put("POS_KEY12", new ServiceRouteConfig("http://localhost:8083", 1000, 50.0f, 10));
        MockServerHttpRequest request = MockServerHttpRequest.post("/api/legacy")
                .body("POS_KEY12_and_more_content");
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        ArgumentCaptor<ServerWebExchange> exchangeCaptor = ArgumentCaptor.forClass(ServerWebExchange.class);
        when(chain.filter(exchangeCaptor.capture())).thenReturn(Mono.empty());

        // When & Then
        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        ServerWebExchange capturedExchange = exchangeCaptor.getValue();
        Object actualUri = capturedExchange.getAttribute(ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR);
        assertEquals("http://localhost:8083", actualUri != null ? actualUri.toString() : "null", "URI should match for positional body");
        verify(circuitBreakerFactory).create("POS_KEY12");
    }

    @Test
    void filter_InvalidKey_ReturnsError() {
        // Given
        MockServerHttpRequest request = MockServerHttpRequest.post("/api/data")
                .body("UNKNOWN_KEY");
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        // When & Then
        StepVerifier.create(filter.filter(exchange, chain))
                .expectError(IllegalArgumentException.class)
                .verify();
    }
    
    @Test
    void filter_RestPathWithQuery_PreservesQuery() {
        // Given
        routingMap.put("QUERY_KEY", new ServiceRouteConfig("http://localhost:8084", 1000, 50.0f, 10));
        MockServerHttpRequest request = MockServerHttpRequest.get("/REST/search?q=test")
                .header("key", "QUERY_KEY")
                .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        ArgumentCaptor<ServerWebExchange> exchangeCaptor = ArgumentCaptor.forClass(ServerWebExchange.class);
        when(chain.filter(exchangeCaptor.capture())).thenReturn(Mono.empty());

        // When & Then
        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        ServerWebExchange capturedExchange = exchangeCaptor.getValue();
        assertEquals("http://localhost:8084/search?q=test", capturedExchange.getAttribute(ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR).toString());
    }
}
