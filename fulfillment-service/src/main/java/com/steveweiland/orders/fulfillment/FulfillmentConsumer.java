package com.steveweiland.orders.fulfillment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.steveweiland.orders.common.JsonMapper;
import com.steveweiland.orders.common.Order;
import com.steveweiland.orders.common.OrderFulfilled;
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
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class FulfillmentConsumer implements Runnable, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(FulfillmentConsumer.class);
    public static final String DEFAULT_TOPIC = "orders";
    public static final String DEFAULT_EVENTS_TOPIC = "order-events";
    public static final String DEFAULT_GROUP_ID = "fulfillment";

    private final KafkaConsumer<String, byte[]> consumer;
    private final ObjectMapper mapper = JsonMapper.shared();
    private final DataSource ds;
    private final ProcessedOrdersStore processedStore;
    private final OutboxStore outboxStore;
    private final DlqProducer dlq;
    private final String topic;
    private final String eventsTopic;
    private volatile boolean running = true;
    private volatile Throwable terminalError;

    public FulfillmentConsumer(String bootstrapServers,
                               DataSource ds,
                               ProcessedOrdersStore processedStore,
                               OutboxStore outboxStore,
                               DlqProducer dlq) {
        this(bootstrapServers, DEFAULT_TOPIC, DEFAULT_EVENTS_TOPIC, DEFAULT_GROUP_ID, Map.of(),
                ds, processedStore, outboxStore, dlq);
    }

    public FulfillmentConsumer(String bootstrapServers,
                               String topic,
                               String eventsTopic,
                               String groupId,
                               Map<String, Object> overrides,
                               DataSource ds,
                               ProcessedOrdersStore processedStore,
                               OutboxStore outboxStore,
                               DlqProducer dlq) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "fulfillment-service");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        props.putAll(overrides);

        this.consumer = new KafkaConsumer<>(props, new StringDeserializer(), new ByteArrayDeserializer());
        this.topic = topic;
        this.eventsTopic = eventsTopic;
        this.ds = ds;
        this.processedStore = processedStore;
        this.outboxStore = outboxStore;
        this.dlq = dlq;
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
        Order order;
        try {
            order = mapper.readValue(rec.value(), Order.class);
        } catch (Exception parseError) {
            try {
                dlq.send(rec, parseError);
            } catch (Exception dlqError) {
                log.error("DLQ publish failed; will redeliver source record", dlqError);
                throw new RuntimeException("DLQ publish failed", dlqError);
            }
            return;
        }

        MDC.put("orderId", order.orderId());
        try {
            log.info("fulfilling customerId={} partition={} offset={}",
                    order.customerId(), rec.partition(), rec.offset());

            Thread.sleep(50);

            try (Connection c = ds.getConnection()) {
                c.setAutoCommit(false);
                try {
                    if (processedStore.exists(c, order.orderId())) {
                        c.commit();
                        log.info("idempotent skip — already fulfilled");
                        return;
                    }
                    Instant fulfilledAt = Instant.now();
                    processedStore.insert(c, order.orderId(), order.customerId(), fulfilledAt);

                    OrderFulfilled event = new OrderFulfilled(order.orderId(), order.customerId(), fulfilledAt);
                    byte[] payload = mapper.writeValueAsBytes(event);
                    outboxStore.insert(c, order.orderId(), eventsTopic, order.orderId(), payload);

                    c.commit();
                    log.info("fulfilled — outbox row enqueued");
                } catch (Exception inner) {
                    c.rollback();
                    throw inner;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("interrupted while fulfilling");
        } catch (Exception e) {
            log.error("fulfillment DB transaction failed; offset will not be committed", e);
            throw new RuntimeException(e);
        } finally {
            MDC.remove("orderId");
        }
    }

    public boolean crashed() {
        return terminalError != null;
    }

    public Throwable terminalError() {
        return terminalError;
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
