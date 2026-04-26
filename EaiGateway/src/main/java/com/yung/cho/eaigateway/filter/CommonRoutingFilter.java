package com.yung.cho.eaigateway.filter;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;

import org.springframework.beans.factory.annotation.Qualifier;
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
import com.yung.cho.eaigateway.util.GatewayLogEvent;
import com.yung.cho.eaigateway.util.GatewayLogProducer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class CommonRoutingFilter implements GlobalFilter, Ordered {

	@Qualifier(value = "routingMap")
	private final Map<String, String> routingMap;
	private final GatewayLogProducer gatewayLogProducer;
	private final ObjectMapper objectMapper;

	@Override
	public int getOrder() {
		return Ordered.LOWEST_PRECEDENCE - 1;
	}

	@Override
	public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
		return DataBufferUtils.join(exchange.getRequest().getBody())
				.defaultIfEmpty(exchange.getResponse().bufferFactory().wrap(new byte[0])).flatMap(dataBuffer -> {
					byte[] bodyBytes = new byte[dataBuffer.readableByteCount()];
					dataBuffer.read(bodyBytes);
					DataBufferUtils.release(dataBuffer);

					byte[] first9 = Arrays.copyOfRange(bodyBytes, 0, Math.min(9, bodyBytes.length));
					byte[] fullBody = Arrays.copyOfRange(bodyBytes, 0, bodyBytes.length);

					String key = new String(first9, StandardCharsets.UTF_8);
					String fullBodyString = new String(fullBody, StandardCharsets.UTF_8);

					sendLog("[REQ1]", key + " : " + resolveTargetUri(key), fullBodyString);

					String newRoute = resolveTargetUri(key);
					if (newRoute == null) {
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
							java.net.URI.create(resolveTargetUri(key)));

					return chain.filter(mutated);
				});
	}

	private String resolveTargetUri(String key) {
		return routingMap.get(key);
	}

	private void sendLog(String phase, String requestUri, String body) {
		try {
			GatewayLogEvent event = new GatewayLogEvent(Instant.now(), phase, requestUri, body);
			gatewayLogProducer.send("gateway-logs", objectMapper.writeValueAsString(event));
		} catch (Exception ignored) {
			ignored.printStackTrace();
		}
	}
}
