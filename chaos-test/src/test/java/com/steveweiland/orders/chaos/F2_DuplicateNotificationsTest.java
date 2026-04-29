package com.steveweiland.orders.chaos;

import com.steveweiland.orders.api.OrderProducer;
import com.steveweiland.orders.common.Order;
import com.steveweiland.orders.common.OrderFulfilled;
import com.steveweiland.orders.common.OrderItem;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * F2 — Duplicate notifications after consumer crash. (spec.md §6 F2)
 *
 * V1 asserted 2 events (uncommitted offset → redelivery on restart). V2 asserts 1:
 * the first run successfully writes to {@code processed_orders} and {@code outbox}
 * in a single DB transaction; the relay publishes; the redelivered record on
 * restart is detected by the idempotency check and skipped.
 *
 * Test mechanism: after the first run, manually reset the consumer-group offset
 * back to 0 (simulating "the V2 consumer crashed before its commitSync"). On
 * restart the record is redelivered and the idempotency table catches it.
 */
@Tag("chaos")
class F2_DuplicateNotificationsTest extends KafkaTestFixture {

    @Test
    void redeliveryAfterCrashIsAbsorbedByIdempotency() throws Exception {
        OrderProducer producer = new OrderProducer(bootstrap(), orderTopic);
        Order order = new Order(
                UUID.randomUUID().toString(),
                "cust-f2",
                List.of(new OrderItem("SKU-A", 1, new BigDecimal("9.99"))),
                new BigDecimal("9.99"),
                Instant.now());
        producer.send(order);

        // First run: process the record + emit to outbox + relay publishes.
        try (V2Stack first = new V2Stack(bootstrap(), orderTopic, eventTopic, dlqTopic,
                groupId, Map.of(), dataSource())) {
            first.start();
            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .until(() -> !TopicTailer.drain(bootstrap(), eventTopic, OrderFulfilled.class,
                            Duration.ofMillis(500)).isEmpty());
        }

        // Simulate the V1 race: consumer crashed AFTER work but BEFORE commitSync.
        // We achieve this by rewinding the committed offset to 0 for this group.
        try (AdminClient admin = adminClient()) {
            Map<TopicPartition, OffsetAndMetadata> rewind = new HashMap<>();
            for (int p = 0; p < 3; p++) rewind.put(new TopicPartition(orderTopic, p), new OffsetAndMetadata(0));
            admin.alterConsumerGroupOffsets(groupId, rewind).all().get(10, TimeUnit.SECONDS);
        }

        // Second run: the original record is redelivered. V2 idempotency check skips it.
        try (V2Stack second = new V2Stack(bootstrap(), orderTopic, eventTopic, dlqTopic,
                groupId, Map.of(), dataSource())) {
            second.start();
            Thread.sleep(3000);

            List<OrderFulfilled> events = TopicTailer.drain(bootstrap(), eventTopic,
                    OrderFulfilled.class, Duration.ofSeconds(2));
            assertEquals(1, events.size(),
                    "V2 fix: idempotency check absorbs redelivery; exactly one event");
        } finally {
            producer.close();
        }
    }
}
