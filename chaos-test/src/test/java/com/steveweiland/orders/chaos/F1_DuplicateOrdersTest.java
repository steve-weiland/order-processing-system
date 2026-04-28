package com.steveweiland.orders.chaos;

import com.steveweiland.orders.api.OrderProducer;
import com.steveweiland.orders.common.Order;
import com.steveweiland.orders.common.OrderFulfilled;
import com.steveweiland.orders.common.OrderItem;
import com.steveweiland.orders.fulfillment.EventProducer;
import com.steveweiland.orders.fulfillment.FulfillmentConsumer;
import com.steveweiland.orders.fulfillment.FulfillmentStore;
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
 * F1 — Duplicate orders (spec.md §6 F1)
 *
 * V1 baseline: this test PASSES by asserting the bug. A client retry produces
 * two records on `orders`, which become two `OrderFulfilled` events.
 *
 * V2 expectation: this test will be inverted (assertEquals 1) once an
 * idempotency-key table is in place. Do not "fix" this test in V1 — its job
 * is to lock in the V1 broken behavior so the V2 diff is unambiguous.
 */
@Tag("chaos")
class F1_DuplicateOrdersTest extends KafkaTestFixture {

    @Test
    void clientRetryCausesDuplicateFulfillmentEvent() throws Exception {
        OrderProducer producer = new OrderProducer(bootstrap(), orderTopic);
        EventProducer eventProducer = new EventProducer(bootstrap(), eventTopic);
        FulfillmentStore store = new FulfillmentStore();
        FulfillmentConsumer consumer = new FulfillmentConsumer(
                bootstrap(), orderTopic, groupId, Map.of(), eventProducer, store);

        Thread worker = new Thread(consumer, "fulfillment-test");
        worker.start();

        try {
            // One logical order, sent twice — simulates the "POST /orders timed out, client retried"
            // path where the same payload reaches the broker twice with the same orderId.
            Order order = new Order(
                    UUID.randomUUID().toString(),
                    "cust-f1",
                    List.of(new OrderItem("SKU-A", 1, new BigDecimal("9.99"))),
                    new BigDecimal("9.99"),
                    Instant.now());

            producer.send(order);
            producer.send(order);

            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(200))
                    .until(() -> TopicTailer.drain(bootstrap(), eventTopic, OrderFulfilled.class,
                            Duration.ofMillis(500)).size() >= 2);

            List<OrderFulfilled> events = TopicTailer.drain(bootstrap(), eventTopic,
                    OrderFulfilled.class, Duration.ofSeconds(2));

            // V1: TWO events for ONE logical order. V2 will assert 1.
            assertEquals(2, events.size(),
                    "V1 baseline: two notifications expected for one duplicate-sent order");
            assertEquals(order.orderId(), events.get(0).orderId());
            assertEquals(order.orderId(), events.get(1).orderId());
        } finally {
            consumer.stop();
            worker.join(10_000);
            producer.close();
            eventProducer.close();
        }
    }
}
