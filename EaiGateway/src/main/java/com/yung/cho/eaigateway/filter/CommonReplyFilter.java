package com.yung.cho.eaigateway.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yung.cho.eaigateway.logging.GatewayLogEvent;
import com.yung.cho.eaigateway.logging.GatewayLogProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.filter.NettyWriteResponseFilter;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.core.scheduler.Scheduler;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;

@Slf4j
@Component
@RequiredArgsConstructor
public class CommonReplyFilter implements GlobalFilter, Ordered {       // response body rewrite 필터 (추후 hopcount 변경용)

    // Cache the ZoneId to prevent thousands of unnecessary lookups per second
    private static final ZoneId SEOUL_ZONE = ZoneId.of("Asia/Seoul");

    private final GatewayLogProducer gatewayLogProducer;
    private final ObjectMapper objectMapper;

    // Create a dedicated thread pool for Kafka logging to prevent thread starvation
    private final Scheduler kafkaLogScheduler = Schedulers.newBoundedElastic(50, 10000, "kafka-response-log-thread");

    @Override
    public int getOrder() {
        // 리액터 체인 바깥쪽
        // CommonReplyFilter ( CommonRoutingFilter () ) --> 여기서 안쪽 필터 에러 Catch 가능
        // -2
        return NettyWriteResponseFilter.WRITE_RESPONSE_FILTER_ORDER - 1;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpResponse originalResponse = exchange.getResponse();
        DataBufferFactory bufferFactory = originalResponse.bufferFactory();

        // request body와 마찬가지로 response body는 휘발성이므로 재주입 과정이 필요함.
        ServerHttpResponseDecorator decoratedResponse = new ServerHttpResponseDecorator(originalResponse) {
            @Override
            public Mono<Void> writeWith(Publisher<? extends DataBuffer> body) {
                if (!(body instanceof Flux<? extends DataBuffer> fluxBody)) {   // response body가 Flux stream인지 체킹
                    return super.writeWith(body);
                }
                return DataBufferUtils.join(body)       // data buffer stream 을 하나의 response body로 합침
                        .flatMap(fullBuffer -> {
                            int originalSize = fullBuffer.readableByteCount();
                            byte[] rewrittenBytes = new byte[originalSize];
                            fullBuffer.read(rewrittenBytes);                                  // Direct Mem 내용을 heap으로 복사
                            if (originalSize > 0) {
                                rewrittenBytes[0] = (byte) '_';                               // 첫 번째 문자를 '_' (ASCII 95) 로 변경
                            }
                            DataBufferUtils.release(fullBuffer);    //*** 해당 Direct Memory release --> 안해주면 off heap 메모리가 서서히 참

                            // 응답 (전문) 카프카 로깅
                            // 별도의 dedicated 쓰레드로 카프카 pub
                            try {
                                Mono.fromRunnable(() -> sendLog("[RES2]", exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR).toString(), rewrittenBytes))
                                        .subscribeOn(kafkaLogScheduler)
                                        .subscribe(null, e -> log.warn("Failed to process Kafka log asynchronously: {}", e.getMessage()));
                            } catch (Exception e) {
                                log.warn("Kafka logging queue is full. Dropping log to protect gateway.");
                            }

                            return super.writeWith(
                                    Mono.just(bufferFactory.wrap(rewrittenBytes))       // heap에 있는 전문을 다시 DataBuffer로 wrap
                            );
                        });
            }
        };

        return chain.filter(exchange.mutate().response(decoratedResponse).build())
                .onErrorResume(ex -> {                    // 해당 + 안쪽 필터 Exception 캐치 및 Resume(클라이언트에 에러 응답 전송)
                            try {
                                Mono.fromRunnable(() ->
                                                sendErrorLog("[ERR1]", exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR).toString(), ex)   // 카프카 에러로그 전송
                                        )
                                        .subscribeOn(kafkaLogScheduler)
                                        .subscribe(null, e -> log.warn("Failed to process Kafka error log asynchronously: {}", e.getMessage()));
                            } catch (Exception e) {
                                log.warn("Kafka logging queue is full. Dropping error log to protect gateway.");
                            }
                            return writeErrorResponse(exchange, HttpStatus.BAD_GATEWAY, ex.getMessage());    // 클라이언트에 보내줄 에러 응답 전문을 다음 filter로 전달.
                        }
                );
    }

    // 카프카 응답 로그
    private void sendLog(String phase, String requestUri, byte[] bodyBytes) {
        try {
            GatewayLogEvent event = new GatewayLogEvent(Instant.now().atZone(SEOUL_ZONE), phase, requestUri, bodyBytes);
            gatewayLogProducer.send("gateway-logs", objectMapper.writeValueAsBytes(event));
        } catch (Exception ignored) {
            log.error("Failed to send Kafka Log", ignored);
        }
    }

    // 카프카 에러 로그
    private void sendErrorLog(String phase, String uri, Throwable ex) {
        try {
            GatewayLogEvent event = new GatewayLogEvent(Instant.now().atZone(SEOUL_ZONE), phase, uri, ("ERROR: " + ex.getMessage()).getBytes(StandardCharsets.UTF_8));
            gatewayLogProducer.send("gateway-logs", objectMapper.writeValueAsBytes(event));
        } catch (Exception ignored) {
            log.error("Failed to send Kafka Error Log", ignored);
        }
    }

    // 클라이언트에게 전달해줄 에러응답 조립
    private Mono<Void> writeErrorResponse(ServerWebExchange exchange, HttpStatus status, String message) {
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        DataBuffer buffer = exchange.getResponse().bufferFactory().wrap(bytes);

        exchange.getResponse().setStatusCode(status);
        exchange.getResponse().getHeaders().setContentType(MediaType.TEXT_PLAIN);
        exchange.getResponse().getHeaders().setContentLength(bytes.length);

        return exchange.getResponse().writeWith(Mono.just(buffer));
    }
}
