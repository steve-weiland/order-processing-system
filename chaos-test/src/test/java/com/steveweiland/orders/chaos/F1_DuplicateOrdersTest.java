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
 * F1 — Duplicate orders. (spec.md §6 F1)
 *
 * V1 asserted 2 events. V2 asserts 1 event: the {@code processed_orders}
 * idempotency check in {@link com.steveweiland.orders.fulfillment.FulfillmentConsumer}
 * absorbs the duplicate before any second outbox row is inserted.
 */
@Tag("chaos")
class F1_DuplicateOrdersTest extends KafkaTestFixture {

    @Test
    void clientRetryProducesSingleFulfillmentEvent() throws Exception {
        OrderProducer producer = new OrderProducer(bootstrap(), orderTopic);
        try (V2Stack stack = new V2Stack(bootstrap(), orderTopic, eventTopic, dlqTopic,
                groupId, Map.of(), dataSource())) {
            stack.start();

            Order order = new Order(
                    UUID.randomUUID().toString(),
                    "cust-f1",
                    List.of(new OrderItem("SKU-A", 1, new BigDecimal("9.99"))),
                    new BigDecimal("9.99"),
                    Instant.now());

            producer.send(order);
            producer.send(order);

            // Wait for the relay to publish at least one event.
            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(200))
                    .until(() -> !TopicTailer.drain(bootstrap(), eventTopic, OrderFulfilled.class,
                            Duration.ofMillis(500)).isEmpty());

            // Give the second delivery time to be processed (and rejected by idempotency).
            Thread.sleep(2000);

            List<OrderFulfilled> events = TopicTailer.drain(bootstrap(), eventTopic,
                    OrderFulfilled.class, Duration.ofSeconds(2));

            assertEquals(1, events.size(),
                    "V2 fix: idempotency check absorbs the duplicate; exactly one event");
            assertEquals(order.orderId(), events.get(0).orderId());
        } finally {
            producer.close();
        }
    }
}
