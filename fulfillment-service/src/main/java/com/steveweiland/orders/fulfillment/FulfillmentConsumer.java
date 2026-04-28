package com.steveweiland.orders.fulfillment;

import com.steveweiland.orders.common.JsonDeserializer;
import com.steveweiland.orders.common.Order;
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
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class FulfillmentConsumer implements Runnable, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(FulfillmentConsumer.class);
    public static final String DEFAULT_TOPIC = "orders";
    public static final String DEFAULT_GROUP_ID = "fulfillment";

    private final KafkaConsumer<String, Order> consumer;
    private final EventProducer events;
    private final FulfillmentStore store;
    private final String topic;
    private volatile boolean running = true;
    private volatile Throwable terminalError;

    public FulfillmentConsumer(String bootstrapServers, EventProducer events, FulfillmentStore store) {
        this(bootstrapServers, DEFAULT_TOPIC, DEFAULT_GROUP_ID, Map.of(), events, store);
    }

    public FulfillmentConsumer(String bootstrapServers,
                               String topic,
                               String groupId,
                               Map<String, Object> overrides,
                               EventProducer events,
                               FulfillmentStore store) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "fulfillment-service");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5_000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.putAll(overrides);

        this.consumer = new KafkaConsumer<>(props, new StringDeserializer(), new JsonDeserializer<>(Order.class));
        this.topic = topic;
        this.events = events;
        this.store = store;
    }

    @Override
    public void run() {
        consumer.subscribe(List.of(topic));
        log.info("subscribed topic={}", topic);
        try {
            while (running) {
                ConsumerRecords<String, Order> batch = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, Order> rec : batch) {
                    processOne(rec);
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

    private void processOne(ConsumerRecord<String, Order> rec) {
        Order order = rec.value();
        MDC.put("orderId", rec.key());
        try {
            log.info("fulfilling customerId={} partition={} offset={}",
                    order.customerId(), rec.partition(), rec.offset());

            Thread.sleep(50);
            store.put(order);

            OrderFulfilled event = new OrderFulfilled(order.orderId(), order.customerId(), Instant.now());
            events.send(event);

            log.info("fulfilled storeSize={}", store.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("interrupted while fulfilling");
        } catch (Exception e) {
            log.error("failed to emit OrderFulfilled", e);
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
