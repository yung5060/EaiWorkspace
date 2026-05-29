package com.yung.cho.eaigateway.filter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.yung.cho.eaigateway.config.ServiceRouteConfig
import com.yung.cho.eaigateway.logging.GatewayLogProducer
import io.github.resilience4j.bulkhead.Bulkhead
import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.bulkhead.BulkheadRegistry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mock
import org.mockito.Mockito.*
import org.mockito.junit.jupiter.MockitoExtension
import org.springframework.cloud.circuitbreaker.resilience4j.ReactiveResilience4JCircuitBreakerFactory
import org.springframework.cloud.client.circuitbreaker.ReactiveCircuitBreaker
import org.springframework.cloud.gateway.filter.GatewayFilterChain
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils
import org.springframework.http.MediaType
import org.springframework.mock.http.server.reactive.MockServerHttpRequest
import org.springframework.mock.web.server.MockServerWebExchange
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
class CommonRoutingFilterTest {

    private lateinit var filter: CommonRoutingFilter
    private lateinit var routingMap: MutableMap<String, ServiceRouteConfig>
    private lateinit var objectMapper: ObjectMapper

    @Mock
    private lateinit var gatewayLogProducer: GatewayLogProducer
    @Mock
    private lateinit var circuitBreakerFactory: ReactiveResilience4JCircuitBreakerFactory
    @Mock
    private lateinit var bulkheadRegistry: BulkheadRegistry
    @Mock
    private lateinit var chain: GatewayFilterChain
    @Mock
    private lateinit var reactiveCircuitBreaker: ReactiveCircuitBreaker
    @Mock
    private lateinit var bulkhead: Bulkhead

    @BeforeEach
    fun setUp() {
        routingMap = mutableMapOf()
        objectMapper = ObjectMapper()
            .registerModule(JavaTimeModule())
            .registerKotlinModule()
        filter = CommonRoutingFilter(routingMap, gatewayLogProducer, objectMapper, circuitBreakerFactory, bulkheadRegistry)

        lenient().`when`(circuitBreakerFactory.create(anyString())).thenReturn(reactiveCircuitBreaker)
        lenient().`when`(bulkheadRegistry.bulkhead(anyString())).thenReturn(bulkhead)
        
        // Mock the bulkhead config to avoid NPE in BulkheadOperator
        val bulkheadConfig = BulkheadConfig.custom().build()
        lenient().`when`(bulkhead.bulkheadConfig).thenReturn(bulkheadConfig)
        lenient().`when`(bulkhead.tryAcquirePermission()).thenReturn(true)
        
        // Mock the circuit breaker run method to just execute the given Mono
        lenient().`when`(reactiveCircuitBreaker.run(any<Mono<Void>>(), any())).thenAnswer { invocation -> invocation.getArgument<Mono<Void>>(0) }
        
        lenient().`when`(chain.filter(any())).thenReturn(Mono.empty())
    }

    @Test
    fun `filter RestPath ExtractsKeyFromHeader`() {
        // Given
        routingMap["TEST_KEY"] = ServiceRouteConfig("http://localhost:8081", 1000, 50.0f, 10)
        val request = MockServerHttpRequest.post("/REST/service")
                .header("key", "TEST_KEY")
                .body("some body content")
        val exchange = MockServerWebExchange.from(request)

        val exchangeCaptor = ArgumentCaptor.forClass(ServerWebExchange::class.java)
        `when`(chain.filter(exchangeCaptor.capture())).thenReturn(Mono.empty())

        // When & Then
        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete()

        val capturedExchange = exchangeCaptor.value
        val actualUri = capturedExchange.getAttribute<Any>(ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR)
        if ("http://localhost:8081/service" != actualUri?.toString()) {
            fail("Expected http://localhost:8081/service but got $actualUri")
        }
        verify(circuitBreakerFactory).create("TEST_KEY")
    }

    @Test
    fun `filter JsonBody ExtractsKeyFromJson`() {
        // Given
        routingMap["JSON_KEY"] = ServiceRouteConfig("http://localhost:8082", 1000, 50.0f, 10)
        val jsonBody = "{\"header_part\": {\"key\": \"JSON_KEY\"}}"
        val request = MockServerHttpRequest.post("/api/data")
                .contentType(MediaType.APPLICATION_JSON)
                .body(jsonBody)
        val exchange = MockServerWebExchange.from(request)

        val exchangeCaptor = ArgumentCaptor.forClass(ServerWebExchange::class.java)
        `when`(chain.filter(exchangeCaptor.capture())).thenReturn(Mono.empty())

        // When & Then
        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete()

        val capturedExchange = exchangeCaptor.value
        val actualUri = capturedExchange.getAttribute<Any>(ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR)
        assertEquals("http://localhost:8082", actualUri?.toString(), "URI should match for JSON body")
        verify(circuitBreakerFactory).create("JSON_KEY")
    }

    @Test
    fun `filter DefaultBody ExtractsKeyFromPrefix`() {
        // Given
        routingMap["POS_KEY12"] = ServiceRouteConfig("http://localhost:8083", 1000, 50.0f, 10)
        val request = MockServerHttpRequest.post("/api/legacy")
                .body("POS_KEY12_and_more_content")
        val exchange = MockServerWebExchange.from(request)

        val exchangeCaptor = ArgumentCaptor.forClass(ServerWebExchange::class.java)
        `when`(chain.filter(exchangeCaptor.capture())).thenReturn(Mono.empty())

        // When & Then
        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete()

        val capturedExchange = exchangeCaptor.value
        val actualUri = capturedExchange.getAttribute<Any>(ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR)
        assertEquals("http://localhost:8083", actualUri?.toString(), "URI should match for positional body")
        verify(circuitBreakerFactory).create("POS_KEY12")
    }

    @Test
    fun `filter InvalidKey ReturnsError`() {
        // Given
        val request = MockServerHttpRequest.post("/api/data")
                .body("UNKNOWN_KEY")
        val exchange = MockServerWebExchange.from(request)

        // When & Then
        StepVerifier.create(filter.filter(exchange, chain))
                .expectError(IllegalArgumentException::class.java)
                .verify()
    }
    
    @Test
    fun `filter RestPathWithQuery PreservesQuery`() {
        // Given
        routingMap["QUERY_KEY"] = ServiceRouteConfig("http://localhost:8084", 1000, 50.0f, 10)
        val request = MockServerHttpRequest.get("/REST/search?q=test")
                .header("key", "QUERY_KEY")
                .build()
        val exchange = MockServerWebExchange.from(request)

        val exchangeCaptor = ArgumentCaptor.forClass(ServerWebExchange::class.java)
        `when`(chain.filter(exchangeCaptor.capture())).thenReturn(Mono.empty())

        // When & Then
        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete()

        val capturedExchange = exchangeCaptor.value
        assertEquals("http://localhost:8084/search?q=test", capturedExchange.getAttribute<Any>(ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR)?.toString())
    }
}
