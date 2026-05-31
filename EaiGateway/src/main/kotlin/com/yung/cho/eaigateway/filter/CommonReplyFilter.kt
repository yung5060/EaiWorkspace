package com.yung.cho.eaigateway.filter

import com.fasterxml.jackson.databind.ObjectMapper
import com.yung.cho.eaigateway.logging.GatewayLogEvent
import com.yung.cho.eaigateway.logging.GatewayLogProducer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactor.asCoroutineDispatcher
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.reactor.mono
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.cloud.gateway.filter.GatewayFilterChain
import org.springframework.cloud.gateway.filter.GlobalFilter
import org.springframework.cloud.gateway.filter.NettyWriteResponseFilter
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils
import org.springframework.core.Ordered
import org.springframework.core.io.buffer.DataBuffer
import org.springframework.core.io.buffer.DataBufferUtils
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.server.reactive.ServerHttpResponseDecorator
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.ZoneId

@Component
class CommonReplyFilter(
    private val gatewayLogProducer: GatewayLogProducer,
    private val objectMapper: ObjectMapper
) : GlobalFilter, Ordered {

    companion object {
        private val log = LoggerFactory.getLogger(CommonReplyFilter::class.java)
        private val SEOUL_ZONE = ZoneId.of("Asia/Seoul")
    }

    // Convert Scheduler to Coroutine Dispatcher for background logging
    private val kafkaDispatcher =
        Schedulers.newBoundedElastic(50, 10000, "kafka-response-log-thread").asCoroutineDispatcher()
    private val logScope = CoroutineScope(SupervisorJob() + kafkaDispatcher)

    override fun getOrder(): Int = NettyWriteResponseFilter.WRITE_RESPONSE_FILTER_ORDER - 1

    override fun filter(exchange: ServerWebExchange, chain: GatewayFilterChain): Mono<Void> = mono {
        val originalResponse = exchange.response
        val bufferFactory = originalResponse.bufferFactory()

        // 1. Decorate the response using a nested mono { } builder for the writeWith method
        val decoratedResponse = object : ServerHttpResponseDecorator(originalResponse) {
            override fun writeWith(body: Publisher<out DataBuffer>): Mono<Void> = mono {
                if (body !is Flux<*>) {
                    super.writeWith(body).awaitSingleOrNull()
                }

                // Suspend and wait for the entire body to be joined
                val fullBuffer = DataBufferUtils.join(body).awaitSingleOrNull()

                if (fullBuffer != null) {
                    val originalSize = fullBuffer.readableByteCount()
                    val rewrittenBytes = ByteArray(originalSize)
                    fullBuffer.read(rewrittenBytes)
                    DataBufferUtils.release(fullBuffer)

                    // Fire-and-forget Kafka Logging
                    logScope.launch {
                        try {
                            val url =
                                exchange.getAttribute<Any>(ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR)?.toString()
                                    ?: "unknown"
                            sendLog("[RES2]", url, rewrittenBytes)
                        } catch (e: Exception) {
                            log.warn("Kafka logging queue is full. Dropping log to protect gateway. Error: ${e.message}")
                        }
                    }

                    // Write the buffered body back to the client
                    super.writeWith(Mono.just(bufferFactory.wrap(rewrittenBytes))).awaitSingleOrNull()
                } else {
                    super.writeWith(Mono.empty()).awaitSingleOrNull()
                }
            }
        }

        val mutatedExchange = exchange.mutate().response(decoratedResponse).build()

        // 2. The entire downstream filter chain is wrapped in a standard try/catch
        try {
            chain.filter(mutatedExchange).awaitSingleOrNull()
        } catch (ex: Exception) {

            // Fire-and-forget Kafka Error Logging
            logScope.launch {
                try {
                    val url = exchange.getAttribute<Any>(ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR)?.toString()
                        ?: "unknown"
                    sendErrorLog("[ERR1]", url, ex)
                } catch (e: Exception) {
                    log.warn("Kafka logging queue is full. Dropping error log to protect gateway. Error: ${e.message}")
                }
            }

            // Suspend and write the custom error response
            writeErrorResponse(exchange, HttpStatus.BAD_GATEWAY, ex.message ?: "Unknown error")

            null
        }
    }

    private fun sendLog(phase: String, requestUri: String, bodyBytes: ByteArray) {
        try {
            val event = GatewayLogEvent(Instant.now().atZone(SEOUL_ZONE), phase, requestUri, bodyBytes)
            gatewayLogProducer.send("gateway-logs", objectMapper.writeValueAsBytes(event))
        } catch (ignored: Exception) {
            log.error("Failed to send Kafka Log", ignored)
        }
    }

    private fun sendErrorLog(phase: String, uri: String, ex: Throwable) {
        try {
            val event = GatewayLogEvent(
                Instant.now().atZone(SEOUL_ZONE),
                phase,
                uri,
                ("ERROR: " + ex.message).toByteArray(StandardCharsets.UTF_8)
            )
            gatewayLogProducer.send("gateway-logs", objectMapper.writeValueAsBytes(event))
        } catch (ignored: Exception) {
            log.error("Failed to send Kafka Error Log", ignored)
        }
    }

    // Convert the error writer into a suspend function
    private suspend fun writeErrorResponse(exchange: ServerWebExchange, status: HttpStatus, message: String) {
        val bytes = message.toByteArray(StandardCharsets.UTF_8)
        val buffer = exchange.response.bufferFactory().wrap(bytes)

        exchange.response.statusCode = status
        exchange.response.headers.contentType = MediaType.TEXT_PLAIN
        exchange.response.headers.contentLength = bytes.size.toLong()

        // Bridge the Reactor write operation into the coroutine
        exchange.response.writeWith(Mono.just(buffer)).awaitSingleOrNull()
    }
}