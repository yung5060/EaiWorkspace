package com.yung.cho.eaigateway.util;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class GatewayLogProducer {

	private final KafkaTemplate<String, String> kafkaTemplate;
	
	public void send(String topic, String message) {
		kafkaTemplate.send(topic, message);
	}
}
