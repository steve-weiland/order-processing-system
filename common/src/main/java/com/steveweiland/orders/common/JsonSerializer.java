package com.steveweiland.orders.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public final class JsonSerializer<T> implements Serializer<T> {
    private final ObjectMapper mapper;

    public JsonSerializer() {
        this(JsonMapper.shared());
    }

    public JsonSerializer(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) return null;
        try {
            return mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new SerializationException("failed to serialize " + data.getClass().getSimpleName(), e);
        }
    }
}
