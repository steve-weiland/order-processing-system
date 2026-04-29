package com.steveweiland.orders.chaos;

import com.steveweiland.orders.common.JsonSerializer;
import com.steveweiland.orders.common.OrderFulfilled;
import com.steveweiland.orders.notification.NotificationConsumer;
import com.steveweiland.orders.notification.ProcessedNotificationsStore;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * F7 — Notification dedup against the relay-crash duplicate-publish window.
 * (spec.md §6 F7)
 *
 * Without notification-side idempotency, an outbox-relay crash between a
 * successful Kafka publish and the {@code markPublishedBatch} UPDATE would
 * cause the surviving relay (or the same one on restart) to republish the
 * same row. Two records on {@code order-events} with the same orderId →
 * two notifications. v2.0.0–v2.2.0 documented this as a known limitation.
 *
 * v2.3.0 closes the window: {@link NotificationConsumer} runs an atomic
 * INSERT into {@code processed_notifications} before logging. The test
 * publishes a duplicate {@code OrderFulfilled} directly to the events topic
 * and asserts only one notification fires.
 */
@Tag("chaos")
class F7_NotificationDedupTest extends KafkaTestFixture {

    @Test
    void duplicateOrderFulfilledFiresNotificationOnce() throws Exception {
        String notifGroup = "notif-" + UUID.randomUUID().toString().substring(0, 8);

        ProcessedNotificationsStore store = new ProcessedNotificationsStore(dataSource());
        NotificationConsumer consumer = new NotificationConsumer(
                bootstrap(), eventTopic, notifGroup, Map.of(), store);

        LinkedBlockingQueue<OrderFulfilled> notified = new LinkedBlockingQueue<>();
        consumer.setHook(notified::offer);

        Thread worker = new Thread(consumer, "notif-f7");
        worker.start();
        try {
            // Publish two identical OrderFulfilled records — the second simulates the
            // republish that would follow an outbox-relay crash between publish and
            // the markPublishedBatch UPDATE.
            String orderId = UUID.randomUUID().toString();
            OrderFulfilled event = new OrderFulfilled(orderId, "cust-f7", Instant.now());

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
            props.put(ProducerConfig.ACKS_CONFIG, "1");
            try (KafkaProducer<String, OrderFulfilled> p = new KafkaProducer<>(
                    props, new StringSerializer(), new JsonSerializer<>())) {
                p.send(new ProducerRecord<>(eventTopic, orderId, event)).get();
                p.send(new ProducerRecord<>(eventTopic, orderId, event)).get();
            }

            // Expect exactly one notification within a short window.
            OrderFulfilled first = notified.poll(10, TimeUnit.SECONDS);
            assertEquals(orderId, first == null ? null : first.orderId(),
                    "first delivery must trigger a notification");

            // Allow the duplicate plenty of time to be (mistakenly) processed.
            // The hook should NOT be called again.
            Awaitility.await()
                    .pollDelay(Duration.ofSeconds(3))
                    .atMost(Duration.ofSeconds(5))
                    .until(() -> true);

            assertEquals(0, notified.size(),
                    "v2.3.0 fix: processed_notifications absorbs the duplicate; no second hook call");
        } finally {
            consumer.stop();
            worker.join(10_000);
        }
    }
}
