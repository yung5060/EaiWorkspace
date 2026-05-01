package com.yung.cho.eaigateway.logging;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class ByteArrayToStringSerializer extends JsonSerializer<byte[]> {
    @Override
    public void serialize(byte[] value, JsonGenerator gen, SerializerProvider serializerProvider) throws IOException {
        // Pipes raw UTF-8 bytes directly into the JSON stream
        gen.writeUTF8String(value, 0, value.length);
    }
}
