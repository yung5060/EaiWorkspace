package com.yung.cho.eaigateway.logging;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class GatewayLogProducer {

	private final KafkaTemplate<String, byte[]> kafkaTemplate;
	
	public void send(String topic, byte[] message) {
		kafkaTemplate.send(topic, message);		// fire and forget...!! 비동기식 publishing
	}
}
