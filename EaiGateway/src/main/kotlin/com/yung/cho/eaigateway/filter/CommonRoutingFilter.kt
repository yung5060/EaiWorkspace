package com.yung.cho.eaigateway.filter

import com.fasterxml.jackson.databind.ObjectMapper
import com.yung.cho.eaigateway.config.ServiceRouteConfig
import com.yung.cho.eaigateway.logging.GatewayLogEvent
import com.yung.cho.eaigateway.logging.GatewayLogProducer
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.cloud.circuitbreaker.resilience4j.ReactiveResilience4JCircuitBreakerFactory
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
import java.time.Instant
import java.time.ZoneId

@Component
class CommonRoutingFilter(
    @Qualifier("routingMap")
    private val routingMap: Map<String, ServiceRouteConfig>,
    private val gatewayLogProducer: GatewayLogProducer,
    private val objectMapper: ObjectMapper,
    private val circuitBreakerFactory: ReactiveResilience4JCircuitBreakerFactory,
    private val bulkheadRegistry: BulkheadRegistry
) : GlobalFilter, Ordered {

    companion object {
        private val log = LoggerFactory.getLogger(CommonRoutingFilter::class.java)
        private val SEOUL_ZONE = ZoneId.of("Asia/Seoul")
    }

    private val kafkaLogScheduler = Schedulers.newBoundedElastic(50, 10000, "kafka-request-log-thread")

    override fun getOrder(): Int {
        // Ordered.LOWEST_PRECEDENCE - 1
        return Int.MAX_VALUE - 1
    }

    override fun filter(exchange: ServerWebExchange, chain: GatewayFilterChain): Mono<Void> {
        return DataBufferUtils.join(exchange.request.body)
            .defaultIfEmpty(exchange.response.bufferFactory().wrap(ByteArray(0)))
            .flatMap { dataBuffer ->
                val bodyBytes = ByteArray(dataBuffer.readableByteCount())
                dataBuffer.read(bodyBytes)
                DataBufferUtils.release(dataBuffer)

                val requestPath = exchange.request.uri.path
                val isRestPath = requestPath.startsWith("/REST/") || requestPath == "/REST"

                val key = if (isRestPath) {
                    exchange.request.headers.getFirst("key")?.replace("[\r\n]".toRegex(), "") ?: ""
                } else {
                    val contentType = exchange.request.headers.contentType
                    if (contentType != null && contentType.includes(MediaType.APPLICATION_JSON)) {
                        if (bodyBytes.isNotEmpty()) {
                            try {
                                val rootNode = objectMapper.readTree(bodyBytes)
                                val keyNode = rootNode.path("header_part").path("key")
                                if (!keyNode.isMissingNode && !keyNode.isNull) {
                                    keyNode.asText()
                                } else ""
                            } catch (e: Exception) {
                                log.warn("Failed to parse routing key from JSON body", e)
                                ""
                            }
                        } else ""
                    } else {
                        String(bodyBytes, 0, minOf(9, bodyBytes.size), StandardCharsets.UTF_8)
                    }
                }

                var targetUri = routingMap[key]?.uri

                if (targetUri != null && isRestPath) {
                    val suffix = if (requestPath.length > 5) requestPath.substring(5) else ""
                    targetUri = UriComponentsBuilder.fromUriString(targetUri)
                        .path(suffix)
                        .query(exchange.request.uri.rawQuery)
                        .build(true)
                        .toUriString()
                }

                try {
                    Mono.fromRunnable<Void> { sendLog("[REQ1]", "$key : $targetUri", bodyBytes) }
                        .subscribeOn(kafkaLogScheduler)
                        .subscribe(null, { e -> log.warn("Failed to process Kafka log asynchronously: {}", e.message) })
                } catch (e: Exception) {
                    log.warn("Kafka logging queue is full. Dropping log to protect gateway.")
                }

                if (targetUri == null) {
                    return@flatMap Mono.error(IllegalArgumentException("No route for key: $key"))
                }

                val decoratedRequest = object : ServerHttpRequestDecorator(exchange.request) {
                    override fun getBody(): Flux<DataBuffer> {
                        return Flux.defer { Mono.just(exchange.response.bufferFactory().wrap(bodyBytes)) }
                    }
                }

                val mutated = exchange.mutate().request(decoratedRequest).build()
                mutated.attributes[ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR] = URI.create(targetUri)

                val cb = circuitBreakerFactory.create(key)
                val bulkhead = bulkheadRegistry.bulkhead(key)

                val routeExecution = chain.filter(mutated)
                    .transformDeferred(BulkheadOperator.of(bulkhead))

                cb.run(routeExecution) { Mono.error(it) }
            }
    }

    private fun sendLog(phase: String, requestUri: String, body: ByteArray) {
        try {
            val event = GatewayLogEvent(Instant.now().atZone(SEOUL_ZONE), phase, requestUri, body)
            gatewayLogProducer.send("gateway-logs", objectMapper.writeValueAsBytes(event))
        } catch (ignored: Exception) {
            log.error("Failed to send Kafka Log", ignored)
        }
    }
}
