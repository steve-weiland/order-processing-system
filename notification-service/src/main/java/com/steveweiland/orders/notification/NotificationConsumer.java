package com.steveweiland.orders.notification;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.steveweiland.orders.common.JsonMapper;
import com.steveweiland.orders.common.OrderFulfilled;
import com.steveweiland.orders.common.dlq.DlqProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

public final class NotificationConsumer implements Runnable, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);
    public static final String DEFAULT_TOPIC = "order-events";
    public static final String DEFAULT_DLQ_TOPIC = "order-events.dlq";
    public static final String DEFAULT_GROUP_ID = "notifications";

    private final KafkaConsumer<String, byte[]> consumer;
    private final ObjectMapper mapper = JsonMapper.shared();
    private final ProcessedNotificationsStore store;
    private final DlqProducer dlq;
    private final String topic;
    private volatile boolean running = true;
    private volatile Throwable terminalError;
    private volatile Consumer<OrderFulfilled> hook = e -> {};

    public NotificationConsumer(String bootstrapServers, DataSource ds) {
        this(bootstrapServers, DEFAULT_TOPIC, DEFAULT_GROUP_ID, Map.of(),
                new ProcessedNotificationsStore(ds),
                new DlqProducer(bootstrapServers, DEFAULT_DLQ_TOPIC));
    }

    public NotificationConsumer(String bootstrapServers,
                                String topic,
                                String groupId,
                                Map<String, Object> overrides,
                                ProcessedNotificationsStore store,
                                DlqProducer dlq) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "notification-service");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.putAll(overrides);

        // F15: deserialize as byte[] and parse manually — the F4 lesson applied
        // to this consumer too. A deserializer that throws inside poll() kills
        // the consumer and stalls the partition on every restart.
        this.consumer = new KafkaConsumer<>(props, new StringDeserializer(),
                new ByteArrayDeserializer());
        this.topic = topic;
        this.store = store;
        this.dlq = dlq;
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
                ConsumerRecords<String, byte[]> batch = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, byte[]> rec : batch) {
                    processOne(rec);
                    consumer.commitSync(Map.of(
                            new TopicPartition(rec.topic(), rec.partition()),
                            new OffsetAndMetadata(rec.offset() + 1)));
                }
            }
        } catch (WakeupException e) {
            if (running) throw e;
        } catch (Throwable t) {
            terminalError = t;
            log.error("consumer terminated by error", t);
            throw t;
        } finally {
            consumer.close(Duration.ofSeconds(10));
            log.info("consumer closed");
        }
    }

    private void processOne(ConsumerRecord<String, byte[]> rec) {
        OrderFulfilled event;
        try {
            event = mapper.readValue(rec.value(), OrderFulfilled.class);
        } catch (Exception parseError) {
            try {
                dlq.send(rec, parseError);
            } catch (Exception dlqError) {
                log.error("DLQ publish failed; will redeliver source record", dlqError);
                throw new RuntimeException("DLQ publish failed", dlqError);
            }
            return;     // offset committed by the caller — partition keeps moving
        }
        notifyOne(event, rec.partition(), rec.offset());
    }

    private void notifyOne(OrderFulfilled event, int partition, long offset) {
        MDC.put("orderId", event.orderId());
        try {
            // Atomic claim — closes the relay-crash duplicate-publish window.
            // True = newly notified; False = duplicate (skip silently).
            if (!store.claim(event.orderId())) {
                log.info("duplicate notification suppressed partition={} offset={}", partition, offset);
                return;
            }
            log.info("Notification sent for order={} customer={} partition={} offset={}",
                    event.orderId(), event.customerId(), partition, offset);
            hook.accept(event);
        } finally {
            MDC.remove("orderId");
        }
    }

    public boolean crashed() {
        return terminalError != null;
    }

    public void stop() {
        running = false;
        consumer.wakeup();
    }

    @Override
    public void close() {
        stop();
        dlq.close();
    }
}
