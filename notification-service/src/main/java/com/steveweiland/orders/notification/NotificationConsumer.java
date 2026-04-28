package com.steveweiland.orders.notification;

import com.steveweiland.orders.common.JsonDeserializer;
import com.steveweiland.orders.common.OrderFulfilled;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

public final class NotificationConsumer implements Runnable, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);
    public static final String DEFAULT_TOPIC = "order-events";
    public static final String DEFAULT_GROUP_ID = "notifications";

    private final KafkaConsumer<String, OrderFulfilled> consumer;
    private final String topic;
    private volatile boolean running = true;
    private volatile Consumer<OrderFulfilled> hook = e -> {};

    public NotificationConsumer(String bootstrapServers) {
        this(bootstrapServers, DEFAULT_TOPIC, DEFAULT_GROUP_ID, Map.of());
    }

    public NotificationConsumer(String bootstrapServers,
                                String topic,
                                String groupId,
                                Map<String, Object> overrides) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "notification-service");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5_000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.putAll(overrides);

        this.consumer = new KafkaConsumer<>(props, new StringDeserializer(),
                new JsonDeserializer<>(OrderFulfilled.class));
        this.topic = topic;
    }

    public void setHook(Consumer<OrderFulfilled> hook) {
        this.hook = hook == null ? e -> {} : hook;
    }

    @Override
    public void run() {
        consumer.subscribe(List.of(topic));
        log.info("subscribed topic={}", topic);
        try {
            while (running) {
                ConsumerRecords<String, OrderFulfilled> batch = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, OrderFulfilled> rec : batch) {
                    notifyOne(rec.value(), rec.partition(), rec.offset());
                }
            }
        } catch (WakeupException e) {
            if (running) throw e;
        } finally {
            consumer.close(Duration.ofSeconds(10));
            log.info("consumer closed");
        }
    }

    private void notifyOne(OrderFulfilled event, int partition, long offset) {
        MDC.put("orderId", event.orderId());
        try {
            log.info("Notification sent for order={} customer={} partition={} offset={}",
                    event.orderId(), event.customerId(), partition, offset);
            hook.accept(event);
        } finally {
            MDC.remove("orderId");
        }
    }

    public void stop() {
        running = false;
        consumer.wakeup();
    }

    @Override
    public void close() {
        stop();
    }
}
