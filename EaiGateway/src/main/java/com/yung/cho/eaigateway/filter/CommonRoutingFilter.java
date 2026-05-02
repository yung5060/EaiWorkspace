package com.yung.cho.eaigateway.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.yung.cho.eaigateway.config.ServiceRouteConfig;
import com.yung.cho.eaigateway.logging.GatewayLogEvent;
import com.yung.cho.eaigateway.logging.GatewayLogProducer;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cloud.circuitbreaker.resilience4j.ReactiveResilience4JCircuitBreakerFactory;
import org.springframework.cloud.client.circuitbreaker.ReactiveCircuitBreaker;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class CommonRoutingFilter implements GlobalFilter, Ordered {        // 라우팅 담당 GlobalFilter

    // Cache the ZoneId to prevent thousands of unnecessary lookups per second
    private static final ZoneId SEOUL_ZONE = ZoneId.of("Asia/Seoul");

    @Qualifier(value = "routingMap")
    private final Map<String, ServiceRouteConfig> routingMap;
    private final GatewayLogProducer gatewayLogProducer;
    private final ObjectMapper objectMapper;
    private final ReactiveResilience4JCircuitBreakerFactory circuitBreakerFactory;
    private final BulkheadRegistry bulkheadRegistry;
    private final Scheduler kafkaLogScheduler = Schedulers.newBoundedElastic(50, 10000, "kafka-request-log-thread");

    @Override
    public int getOrder() {
        // 리액터 체인 가장 안쪽
        // CommonReplyFilter ( CommonRoutingFilter () ) --> 해당 필터 에러는 바깥쪽 필터에서 Catch 가능
        // 2147483646
        return Ordered.LOWEST_PRECEDENCE - 1;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        return DataBufferUtils.join(exchange.getRequest().getBody())        // DataBuffer stream을 하나의 data block(full request body)로 합쳐줌
                .defaultIfEmpty(exchange.getResponse().bufferFactory().wrap(new byte[0])) // request body 없을시에도 chain이 동작하도록
                .flatMap(dataBuffer -> {
                    byte[] bodyBytes = new byte[dataBuffer.readableByteCount()];
                    dataBuffer.read(bodyBytes);                 //*** Direct Mem에 존재하는 request body를 heap으로 복사
                    DataBufferUtils.release(dataBuffer);        //*** 해당 Direct Memory release --> 안해주면 off heap 메모리가 서서히 참
                    
                    String key;
                    String requestPath = exchange.getRequest().getURI().getPath();
                    boolean isRestPath = requestPath.startsWith("/REST/") || requestPath.equals("/REST");

                    if (isRestPath) {
                        // REST 경로인 경우 Body 검사를 바이패스하고 헤더에서 라우팅 키를 추출
                        key = exchange.getRequest().getHeaders().getFirst("key");
                        if (key == null) key = "";
                    } else {
                        MediaType contentType = exchange.getRequest().getHeaders().getContentType();

                        // Content-Type이 JSON인 경우 지정된 HTTP Body Json에서 라우팅 키를 추출
                        if (contentType != null && contentType.includes(MediaType.APPLICATION_JSON)) {
                            try {
                                JsonNode rootNode = objectMapper.readTree(bodyBytes);
                                JsonNode keyNode = rootNode.path("header_part").path("key");
                                
                                if (!keyNode.isMissingNode() && !keyNode.isNull()) {
                                    key = keyNode.asText();
                                } else {
                                    key = ""; // 구조가 다르거나 key가 없을 때의 기본값
                                }
                            } catch (Exception e) {
                                log.warn("Failed to parse routing key from JSON body", e);
                                key = "";
                            }
                        } else {
                            // 그 외의 경우 기존처럼 body의 앞 9바이트에서 추출
                            key = new String(bodyBytes, 0, Math.min(9, bodyBytes.length), StandardCharsets.UTF_8);  // 라우팅 key
                        }
                    }

//                    String fullBodyString = new String(fullBody, StandardCharsets.UTF_8);   // 전체 전문

                    String targetUri = Optional.ofNullable(routingMap.get(key))
                            .map(ServiceRouteConfig::uri)
                            .orElse(null);

                    if (targetUri != null && isRestPath) {
                        // "/REST" 부분을 제외한 나머지 경로 추출
                        String suffix = requestPath.length() > 5 ? requestPath.substring(5) : "";
                        
                        // 기존 목적지 URI에 동적으로 Path와 Query String을 붙여줌
                        targetUri = UriComponentsBuilder.fromUriString(targetUri)
                                .path(suffix)
                                .query(exchange.getRequest().getURI().getRawQuery())
                                .build(true)
                                .toUriString();
                    }

                    try {
                        // 별도의 kafkaLogScheduler 쓰레드로 카프카 pub
                        String finalKey = key;
                        String finalTargetUri = targetUri;
                        Mono.fromRunnable(() -> sendLog("[REQ1]", finalKey + " : " + finalTargetUri, bodyBytes))
                                .subscribeOn(kafkaLogScheduler)
                                .subscribe(null, e -> log.warn("Failed to process Kafka log asynchronously: {}", e.getMessage()));
                    } catch (Exception e) {
                        log.warn("Kafka logging queue is full. Dropping log to protect gateway.");
                    }

                    if (targetUri == null) {
                        return Mono.error(new IllegalArgumentException("No route for key: " + key));    // Outer 필터에서 catch 하려면 리액터 error로 반환해야함
                    }

                    // request body는 chain에서 휘발성이기 때문에 한번 읽으면 다음 chain으로 전파가 안됨.
                    // 따라서 전문을 request chain에 재주입 해줘야함.
                    ServerHttpRequest decoratedRequest = new ServerHttpRequestDecorator(exchange.getRequest()) {
                        @Override
                        public Flux<DataBuffer> getBody() {     // original (drained) 호출 대신 override된 getBody() 호출되도록.
                            // heap에 있는 전문을 다시 DataBuffer로 wrap
                            // defer() -> 매 전문에 따라 fresh stream(DataBuffer) 생성되도록
                            return Flux.defer(() -> Mono.just(exchange.getResponse().bufferFactory().wrap(bodyBytes)));
                        }
                    };

                    // 휘발된 전문 가진 original exchange를 새롭게 전문을 주입한 mutated exchange로 교체
                    ServerWebExchange mutated = exchange.mutate().request(decoratedRequest).build();

                    // 라우팅 (targetUri) 설정
                    mutated.getAttributes().put(ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR,
                            java.net.URI.create(targetUri));

                    // 해당 라우트에 대한 서킷브레이커 + 세션매니저 get
                    ReactiveCircuitBreaker cb = circuitBreakerFactory.create(key);
                    Bulkhead bulkhead = bulkheadRegistry.bulkhead(key);

                    // 리액터 체인에 세션매니저 옵션 추가
                    Mono<Void> routeExecution = chain.filter(mutated)
                            .transformDeferred(BulkheadOperator.of(bulkhead));

                    // 리액터 체인을 서킷브레이커에 wrap 한 후 return
                    return cb.run(
                            routeExecution
                            , Mono::error
                    );
                });
    }

    // 카프카 요청 로그
    private void sendLog(String phase, String requestUri, byte[] body) {
        try {
            // Pass byte[] directly. GatewayLogEvent requires a signature update.
            GatewayLogEvent event = new GatewayLogEvent(Instant.now().atZone(SEOUL_ZONE), phase, requestUri, body);
            gatewayLogProducer.send("gateway-logs", objectMapper.writeValueAsBytes(event));
        } catch (Exception ignored) {
            log.error("Failed to send Kafka Log", ignored);     // 카프카 pub은 transaction에 영향이 없도록 exception 무시
        }
    }
}
