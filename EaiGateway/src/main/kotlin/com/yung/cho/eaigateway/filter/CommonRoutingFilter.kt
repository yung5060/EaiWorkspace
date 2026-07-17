package com.yung.cho.eaigateway.filter

import com.fasterxml.jackson.databind.ObjectMapper
import com.yung.cho.eaigateway.config.ServiceRouteConfig
import com.yung.cho.eaigateway.logging.GatewayLogEvent
import com.yung.cho.eaigateway.logging.GatewayLogProducer
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactor.asCoroutineDispatcher
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.cloud.gateway.filter.GatewayFilterChain
import org.springframework.cloud.gateway.filter.GlobalFilter
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils
import org.springframework.core.Ordered
import org.springframework.core.io.buffer.DataBuffer
import org.springframework.core.io.buffer.DataBufferUtils
import org.springframework.http.MediaType
import org.springframework.http.server.reactive.ServerHttpRequestDecorator
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import org.springframework.web.util.UriComponentsBuilder
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import java.time.ZoneId

@Component
class CommonRoutingFilter(
    @Qualifier("routingMap")
    private val routingMap: Map<String, ServiceRouteConfig>,
    private val gatewayLogProducer: GatewayLogProducer,
    private val objectMapper: ObjectMapper,
    private val circuitbreakerRegistry: CircuitBreakerRegistry,
    private val bulkheadRegistry: BulkheadRegistry
) : GlobalFilter, Ordered {

    companion object {
        private val log = LoggerFactory.getLogger(CommonRoutingFilter::class.java)
        private val SEOUL_ZONE = ZoneId.of("Asia/Seoul")
    }

    // Convert the Reactor Scheduler directly into a Coroutine Dispatcher
    private val kafkaDispatcher =
        Schedulers.newBoundedElastic(50, 10000, "kafka-request-log-thread").asCoroutineDispatcher()

    // Create a dedicated scope for fire-and-forget background tasks
    private val logScope = CoroutineScope(SupervisorJob() + kafkaDispatcher)

//    override fun getOrder(): Int = Int.MAX_VALUE - 1
    override fun getOrder(): Int = Ordered.LOWEST_PRECEDENCE - 1

    private fun sendLog(phase: String, requestUri: String, body: ByteArray) {
        try {
            val event = GatewayLogEvent(Instant.now().atZone(SEOUL_ZONE), phase, requestUri, body)
            gatewayLogProducer.send("gateway-logs", objectMapper.writeValueAsBytes(event))
        } catch (ignored: Exception) {
            log.error("Failed to send Kafka Log", ignored)
        }
    }

    override fun filter(exchange: ServerWebExchange, chain: GatewayFilterChain): Mono<Void> = mono {

        // 1. Await the body buffer non-blockingly, avoiding flatMap
        val dataBuffer = DataBufferUtils.join(exchange.request.body).awaitSingleOrNull()

        val bodyBytes = if (dataBuffer != null) {
            val bytes = ByteArray(dataBuffer.readableByteCount())
            dataBuffer.read(bytes)
            DataBufferUtils.release(dataBuffer)
            bytes
        } else {
            ByteArray(0)
        }

        // 2. Extract key
        val requestPath = exchange.request.uri.path
        val isRestPath = requestPath.startsWith("/REST/") || requestPath == "/REST"

        val key = if (isRestPath) {
            exchange.request.headers.getFirst("key")?.replace("[\r\n]".toRegex(), "") ?: ""
        } else {
            val contentType = exchange.request.headers.contentType
            if (contentType != null && contentType.includes(MediaType.APPLICATION_JSON) && bodyBytes.isNotEmpty()) {
                try {
                    val rootNode = objectMapper.readTree(bodyBytes)
                    val keyNode = rootNode.path("header_part").path("key")
                    if (!keyNode.isMissingNode && !keyNode.isNull) keyNode.asText() else ""
                } catch (e: Exception) {
                    log.warn("Failed to parse routing key from JSON body", e)
                    ""
                }
            } else {
                String(bodyBytes, 0, minOf(9, bodyBytes.size), StandardCharsets.UTF_8)
            }
        }

        // 3. Resolve Target URI
        var targetUri = routingMap[key]?.uri ?: throw IllegalArgumentException("No route for key: $key")

        if (isRestPath) {
            val suffix = if (requestPath.length > 5) requestPath.substring(5) else ""
            targetUri = UriComponentsBuilder.fromUriString(targetUri)
                .path(suffix)
                .query(exchange.request.uri.rawQuery)
                .build(true)
                .toUriString()
        }

        // 4. Fire-and-forget Kafka Logging using Coroutine launch
        logScope.launch {
            try {
                sendLog("[REQ1]", "$key : $targetUri", bodyBytes)
            } catch (e: Exception) {
                log.warn("Kafka logging queue is full. Dropping log to protect gateway. Error: ${e.message}")
            }
        }

        // 5. Decorate Request and Mutate Exchange
        val decoratedRequest = object : ServerHttpRequestDecorator(exchange.request) {
            override fun getBody(): Flux<DataBuffer> =
                Flux.defer { Mono.just(exchange.response.bufferFactory().wrap(bodyBytes)) }
        }

        val mutated = exchange.mutate().request(decoratedRequest).build()
        mutated.attributes[ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR] = URI.create(targetUri)

        // 6. Apply Resilience4j and await the final chain execution
        val cb = circuitbreakerRegistry.circuitBreaker(key)
        val bulkhead = bulkheadRegistry.bulkhead(key)

        // Return the final suspended result
        chain.filter(mutated)
            .timeout(Duration.ofMillis(70000))
            .transformDeferred(CircuitBreakerOperator.of(cb))
            .transformDeferred(BulkheadOperator.of(bulkhead))
            .awaitSingleOrNull()
    }
}