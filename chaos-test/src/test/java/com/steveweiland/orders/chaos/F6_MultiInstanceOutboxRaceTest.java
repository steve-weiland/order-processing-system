package com.steveweiland.orders.chaos;

import com.steveweiland.orders.api.OrderProducer;
import com.steveweiland.orders.common.Order;
import com.steveweiland.orders.common.OrderFulfilled;
import com.steveweiland.orders.common.OrderItem;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * F6 — Multi-instance outbox-poll race. (spec.md §6 F6)
 *
 * F6 is new in v2.2.0. It does not exist in V1 (no outbox), v2.0.0, or
 * v2.1.0 (single-instance fulfillment). It surfaces only when more than
 * one fulfillment-service JVM polls the same Postgres outbox.
 *
 * <p>Without {@code SELECT FOR UPDATE SKIP LOCKED}, two relay instances
 * polling concurrently both pick up rows 1..N before either calls
 * {@code UPDATE published_at}, both publish to Kafka, and every event is
 * duplicated. With {@code FOR UPDATE SKIP LOCKED} held inside an explicit
 * transaction across the publish + mark-published cycle, each row is
 * locked by exactly one relay and the other instance skips it.
 *
 * Test mechanism: spawn two {@link V2Stack}s sharing the same broker and
 * Postgres, send N orders, assert exactly N {@code OrderFulfilled} events.
 */
@Tag("chaos")
class F6_MultiInstanceOutboxRaceTest extends KafkaTestFixture {

    private static final int ORDERS = 50;

    @Test
    void twoRelaysPublishEachOrderExactlyOnce() throws Exception {
        OrderProducer producer = new OrderProducer(bootstrap(), orderTopic);
        for (int i = 0; i < ORDERS; i++) {
            Order order = new Order(
                    UUID.randomUUID().toString(),
                    "cust-f6-" + i,
                    List.of(new OrderItem("SKU-A", 1, new BigDecimal("1.00"))),
                    new BigDecimal("1.00"),
                    Instant.now());
            producer.send(order);
        }
        producer.close();

        try (V2Stack a = new V2Stack(bootstrap(), orderTopic, eventTopic, dlqTopic,
                                     groupId, Map.of(), dataSource());
             V2Stack b = new V2Stack(bootstrap(), orderTopic, eventTopic, dlqTopic,
                                     groupId, Map.of(), dataSource())) {
            a.start();
            b.start();

            Awaitility.await()
                    .atMost(Duration.ofSeconds(20))
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> TopicTailer.drain(bootstrap(), eventTopic, OrderFulfilled.class,
                            Duration.ofMillis(500)).size() >= ORDERS);

            // Allow extra time for any duplicate publishes to surface before draining.
            Thread.sleep(2000);

            List<OrderFulfilled> events = TopicTailer.drain(bootstrap(), eventTopic,
                    OrderFulfilled.class, Duration.ofSeconds(2));

            assertEquals(ORDERS, events.size(),
                    "v2.2.0 fix: SELECT FOR UPDATE SKIP LOCKED prevents duplicate publishes "
                            + "when multiple relay instances poll the same outbox");
        }
    }
}
