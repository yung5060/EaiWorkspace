package com.yung.cho.eaigateway.logging

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import java.io.IOException

class ByteArrayToStringSerializer : JsonSerializer<ByteArray>() {
    @Throws(IOException::class)
    override fun serialize(value: ByteArray, gen: JsonGenerator, serializerProvider: SerializerProvider) {
        // Pipes raw UTF-8 bytes directly into the JSON stream
        gen.writeUTF8String(value, 0, value.size)
    }
}
