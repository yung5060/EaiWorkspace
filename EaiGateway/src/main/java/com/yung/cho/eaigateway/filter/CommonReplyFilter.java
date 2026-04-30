package com.yung.cho.eaigateway.filter;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;

import org.reactivestreams.Publisher;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.filter.NettyWriteResponseFilter;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yung.cho.eaigateway.logging.GatewayLogEvent;
import com.yung.cho.eaigateway.logging.GatewayLogProducer;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class CommonReplyFilter implements GlobalFilter, Ordered {

    private final GatewayLogProducer gatewayLogProducer;
    private final ObjectMapper objectMapper;

    @Override
    public int getOrder() {
        return NettyWriteResponseFilter.WRITE_RESPONSE_FILTER_ORDER - 1;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpResponse originalResponse = exchange.getResponse();
        DataBufferFactory bufferFactory = originalResponse.bufferFactory();

        ServerHttpResponseDecorator decoratedResponse = new ServerHttpResponseDecorator(originalResponse) {
            @Override
            public Mono<Void> writeWith(Publisher<? extends DataBuffer> body) {
                if (!(body instanceof Flux<? extends DataBuffer> fluxBody)) {
                    return super.writeWith(body);
                }
                return DataBufferUtils.join(body)
                        .flatMap(fullBuffer -> {
                            byte[] bytes = new byte[fullBuffer.readableByteCount()];
                            fullBuffer.read(bytes);
                            DataBufferUtils.release(fullBuffer);

                            String newResponseBody = new String(bytes, StandardCharsets.UTF_8).toLowerCase();
                            sendLog("[RES2]", "gatewayResponse", newResponseBody);

                            byte[] rewrittenBytes = newResponseBody.getBytes(StandardCharsets.UTF_8);
                            return super.writeWith(
                                    Mono.just(bufferFactory.wrap(rewrittenBytes))
                            );
                        });
            }
        };

        return chain.filter(exchange.mutate().response(decoratedResponse).build()).onErrorResume(ex ->
                sendErrorLog("[ERR0]", "gatewayError", ex)
                        .then(writeErrorResponse(exchange, HttpStatus.BAD_GATEWAY, ex.getMessage()))
        );
    }

    private void sendLog(String phase, String requestUri, String body) {
        try {
            GatewayLogEvent event = new GatewayLogEvent(Instant.now().atZone(ZoneId.of("Asia/Seoul")), phase, requestUri, body);
            gatewayLogProducer.send("gateway-logs", objectMapper.writeValueAsString(event));
        } catch (Exception ignored) {
            ignored.printStackTrace();
        }
    }

    private Mono<Void> sendErrorLog(String phase, String uri, Throwable ex) {
        try {
            GatewayLogEvent event = new GatewayLogEvent(Instant.now().atZone(ZoneId.of("Asia/Seoul")), phase, uri, "ERROR: " + ex.getMessage());
            gatewayLogProducer.send("gateway-logs", objectMapper.writeValueAsString(event));
        } catch (Exception ignored) {
        }
        return Mono.empty();
    }

    private Mono<Void> writeErrorResponse(ServerWebExchange exchange, HttpStatus status, String message) {
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        DataBuffer buffer = exchange.getResponse().bufferFactory().wrap(bytes);

        exchange.getResponse().setStatusCode(status);
        exchange.getResponse().getHeaders().setContentType(MediaType.TEXT_PLAIN);
        exchange.getResponse().getHeaders().setContentLength(bytes.length);

        return exchange.getResponse().writeWith(Mono.just(buffer));
    }
}
