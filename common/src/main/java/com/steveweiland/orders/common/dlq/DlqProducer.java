package com.steveweiland.orders.common.dlq;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public final class DlqProducer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(DlqProducer.class);

    private final KafkaProducer<byte[], byte[]> producer;
    private final String topic;

    public DlqProducer(String bootstrapServers, String topic) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "dlq-" + topic);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        this.producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());
        this.topic = topic;
    }

    public void send(ConsumerRecord<String, byte[]> source, Throwable cause) throws Exception {
        byte[] keyBytes = source.key() == null ? null : source.key().getBytes(StandardCharsets.UTF_8);
        ProducerRecord<byte[], byte[]> rec = new ProducerRecord<>(topic, keyBytes, source.value());
        Headers h = rec.headers();
        h.add("x-dlq-reason", (cause.getClass().getSimpleName() + ": " + cause.getMessage()).getBytes(StandardCharsets.UTF_8));
        h.add("x-dlq-source-topic", source.topic().getBytes(StandardCharsets.UTF_8));
        h.add("x-dlq-source-partition", String.valueOf(source.partition()).getBytes(StandardCharsets.UTF_8));
        h.add("x-dlq-source-offset", String.valueOf(source.offset()).getBytes(StandardCharsets.UTF_8));
        h.add("x-dlq-original-timestamp", String.valueOf(source.timestamp()).getBytes(StandardCharsets.UTF_8));
        producer.send(rec).get(10, TimeUnit.SECONDS);
        log.warn("routed to DLQ source={}/{}@{} reason={}", source.topic(), source.partition(), source.offset(),
                cause.getClass().getSimpleName());
    }

    @Override
    public void close() {
        producer.close(Duration.ofSeconds(10));
    }
}
