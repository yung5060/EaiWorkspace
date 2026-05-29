package com.yung.cho.eaigateway.filter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.yung.cho.eaigateway.logging.GatewayLogProducer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.eq
import org.mockito.Mock
import org.mockito.Mockito.*
import org.mockito.junit.jupiter.MockitoExtension
import org.springframework.cloud.gateway.filter.GatewayFilterChain
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils
import org.springframework.core.io.buffer.DefaultDataBufferFactory
import org.springframework.http.HttpStatus
import org.springframework.mock.http.server.reactive.MockServerHttpRequest
import org.springframework.mock.web.server.MockServerWebExchange
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.net.URI
import java.nio.charset.StandardCharsets

@ExtendWith(MockitoExtension::class)
class CommonReplyFilterTest {

    private lateinit var filter: CommonReplyFilter
    private lateinit var objectMapper: ObjectMapper

    @Mock
    private lateinit var gatewayLogProducer: GatewayLogProducer
    @Mock
    private lateinit var chain: GatewayFilterChain

    @BeforeEach
    fun setUp() {
        objectMapper = ObjectMapper()
            .registerModule(JavaTimeModule())
            .registerKotlinModule()
        filter = CommonReplyFilter(gatewayLogProducer, objectMapper)
    }

    @Test
    fun `filter SuccessfulResponse LogsToKafka`() {
        // Given
        val request = MockServerHttpRequest.get("/api/test").build()
        val exchange = MockServerWebExchange.from(request)
        exchange.attributes[ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR] = URI.create("http://backend/api/test")

        `when`(chain.filter(any())).thenAnswer { invocation ->
            val ex = invocation.getArgument<ServerWebExchange>(0)
            val buffer = DefaultDataBufferFactory().wrap("response body".toByteArray(StandardCharsets.UTF_8))
            ex.response.writeWith(Flux.just(buffer))
        }

        // When & Then
        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete()

        // Verify Kafka logging was called (it's async so we might need a small timeout or just verify)
        verify(gatewayLogProducer, timeout(1000)).send(eq("gateway-logs"), any())
    }

    @Test
    fun `filter ChainError LogsToKafkaAndReturnsBadGateway`() {
        // Given
        val request = MockServerHttpRequest.get("/api/test").build()
        val exchange = MockServerWebExchange.from(request)
        exchange.attributes[ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR] = URI.create("http://backend/api/test")

        `when`(chain.filter(any())).thenReturn(Mono.error(RuntimeException("Test Error")))

        // When & Then
        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete()

        assertEquals(HttpStatus.BAD_GATEWAY, exchange.response.statusCode)
        verify(gatewayLogProducer, timeout(1000)).send(eq("gateway-logs"), any())
    }
}
