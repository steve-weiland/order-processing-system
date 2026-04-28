package com.steveweiland.orders.fulfillment;

import com.steveweiland.orders.common.JsonSerializer;
import com.steveweiland.orders.common.OrderFulfilled;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class EventProducer implements AutoCloseable {
    public static final String DEFAULT_TOPIC = "order-events";

    private final KafkaProducer<String, OrderFulfilled> producer;
    private final String topic;

    public EventProducer(String bootstrapServers) {
        this(bootstrapServers, DEFAULT_TOPIC);
    }

    public EventProducer(String bootstrapServers, String topic) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "fulfillment-service");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
        this.producer = new KafkaProducer<>(props, new StringSerializer(), new JsonSerializer<>());
        this.topic = topic;
    }

    public void send(OrderFulfilled event) throws InterruptedException, ExecutionException, TimeoutException {
        ProducerRecord<String, OrderFulfilled> record = new ProducerRecord<>(topic, event.orderId(), event);
        producer.send(record).get(10, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        producer.close(Duration.ofSeconds(10));
    }
}
