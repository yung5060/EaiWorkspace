package com.yung.cho.eaigateway.logging

import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

@Service
class GatewayLogProducer(
    private val kafkaTemplate: KafkaTemplate<String, ByteArray>
) {
    fun send(topic: String, message: ByteArray) {
        kafkaTemplate.send(topic, message) // fire and forget...!! 비동기식 publishing
    }
}
