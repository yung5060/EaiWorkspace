package com.yung.cho.eaigateway.filter;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import com.yung.cho.eaigateway.config.ServiceRouteConfig;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cloud.circuitbreaker.resilience4j.ReactiveResilience4JCircuitBreakerFactory;
import org.springframework.cloud.client.circuitbreaker.ReactiveCircuitBreaker;
import org.springframework.cloud.client.circuitbreaker.ReactiveCircuitBreakerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
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
public class CommonRoutingFilter implements GlobalFilter, Ordered {

    @Qualifier(value = "routingMap")
    private final Map<String, ServiceRouteConfig> routingMap;
    private final GatewayLogProducer gatewayLogProducer;
    private final ObjectMapper objectMapper;
    private final ReactiveResilience4JCircuitBreakerFactory circuitBreakerFactory;
    private final BulkheadRegistry bulkheadRegistry;

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE - 1;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        return DataBufferUtils.join(exchange.getRequest().getBody())
                .defaultIfEmpty(exchange.getResponse().bufferFactory().wrap(new byte[0]))
                .flatMap(dataBuffer -> {
                    byte[] bodyBytes = new byte[dataBuffer.readableByteCount()];
                    dataBuffer.read(bodyBytes);
                    DataBufferUtils.release(dataBuffer);

                    byte[] first9 = Arrays.copyOfRange(bodyBytes, 0, Math.min(9, bodyBytes.length));
                    byte[] fullBody = Arrays.copyOfRange(bodyBytes, 0, bodyBytes.length);

                    String key = new String(first9, StandardCharsets.UTF_8);
                    String fullBodyString = new String(fullBody, StandardCharsets.UTF_8);

                    String targetUri = Optional.ofNullable(routingMap.get(key))
                            .map(ServiceRouteConfig::uri)
                            .orElse(null);

                    sendLog("[REQ1]", key + " : " + targetUri, fullBodyString);

                    if (targetUri == null) {
                        return Mono.error(new IllegalArgumentException("No route for key: " + key));
                    }

                    ServerHttpRequest decoratedRequest = new ServerHttpRequestDecorator(exchange.getRequest()) {
                        @Override
                        public Flux<DataBuffer> getBody() {
                            return Flux.defer(() -> Mono.just(exchange.getResponse().bufferFactory().wrap(bodyBytes)));
                        }
                    };

                    ServerWebExchange mutated = exchange.mutate().request(decoratedRequest).build();

                    mutated.getAttributes().put(ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR,
                            java.net.URI.create(targetUri));

                    ReactiveCircuitBreaker cb = circuitBreakerFactory.create(key);
                    Bulkhead bulkhead = bulkheadRegistry.bulkhead(key);

                    Mono<Void> routeExecution = chain.filter(mutated)
                            .transformDeferred(BulkheadOperator.of(bulkhead));

                    return cb.run(
                            routeExecution
                            , Mono::error
                    );
                });
    }

    private void sendLog(String phase, String requestUri, String body) {
        try {
            GatewayLogEvent event = new GatewayLogEvent(Instant.now().atZone(ZoneId.of("Asia/Seoul")), phase, requestUri, body);
            gatewayLogProducer.send("gateway-logs", objectMapper.writeValueAsString(event));
        } catch (Exception ignored) {
            ignored.printStackTrace();
        }
    }
}
