package com.steveweiland.orders.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public final class JsonDeserializer<T> implements Deserializer<T> {
    private final Class<T> type;
    private final ObjectMapper mapper;

    public JsonDeserializer(Class<T> type) {
        this(type, JsonMapper.shared());
    }

    public JsonDeserializer(Class<T> type, ObjectMapper mapper) {
        this.type = type;
        this.mapper = mapper;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) return null;
        try {
            return mapper.readValue(data, type);
        } catch (IOException e) {
            throw new SerializationException("failed to deserialize " + type.getSimpleName(), e);
        }
    }
}
