package com.yung.cho.eaigateway.filter;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;

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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yung.cho.eaigateway.logging.GatewayLogEvent;
import com.yung.cho.eaigateway.logging.GatewayLogProducer;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class CommonReplyFilter implements GlobalFilter, Ordered {       // response body rewrite 필터 (추후 hopcount 변경용)

    private final GatewayLogProducer gatewayLogProducer;
    private final ObjectMapper objectMapper;

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
                            byte[] bytes = new byte[fullBuffer.readableByteCount()];
                            fullBuffer.read(bytes);                 //*** Direct Mem에 존재하는 response body를 heap으로 복사
                            DataBufferUtils.release(fullBuffer);    //*** 해당 Direct Memory release --> 안해주면 off heap 메모리가 서서히 참

                            String newResponseBody = new String(bytes, StandardCharsets.UTF_8).toLowerCase();       // body rewrite

                            sendLog("[RES2]", exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR).toString(), newResponseBody);     // 응답 (전문) 카프카 로깅

                            byte[] rewrittenBytes = newResponseBody.getBytes(StandardCharsets.UTF_8);       // 전문을 byte[]로 변환
                            return super.writeWith(
                                    Mono.just(bufferFactory.wrap(rewrittenBytes))       // heap에 있는 전문을 다시 DataBuffer로 wrap
                            );
                        });
            }
        };

        return chain.filter(exchange.mutate().response(decoratedResponse).build())
                .onErrorResume(ex ->                    // 해당 + 안쪽 필터 Exception 캐치 및 Resume(클라이언트에 에러 응답 전송)
                        sendErrorLog("[ERR0]", "gatewayError", ex)      // 카프카 에러로그 전송
                                .then(writeErrorResponse(exchange, HttpStatus.BAD_GATEWAY, ex.getMessage()))    // 클라이언트에 보내줄 에러 응답 전문을 다음 filter로 전달.
                );
    }

    // 카프카 응답 로그
    private void sendLog(String phase, String requestUri, String body) {
        try {
            GatewayLogEvent event = new GatewayLogEvent(Instant.now().atZone(ZoneId.of("Asia/Seoul")), phase, requestUri, body);
            gatewayLogProducer.send("gateway-logs", objectMapper.writeValueAsString(event));
        } catch (Exception ignored) {
            ignored.printStackTrace();
        }
    }

    // 카프카 에러 로그
    private Mono<Void> sendErrorLog(String phase, String uri, Throwable ex) {
        try {
            GatewayLogEvent event = new GatewayLogEvent(Instant.now().atZone(ZoneId.of("Asia/Seoul")), phase, uri, "ERROR: " + ex.getMessage());
            gatewayLogProducer.send("gateway-logs", objectMapper.writeValueAsString(event));
        } catch (Exception ignored) {
            ignored.printStackTrace();
        }
        return Mono.empty();
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
