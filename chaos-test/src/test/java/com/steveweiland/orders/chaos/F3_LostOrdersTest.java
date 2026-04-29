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
 * F3 — Lost orders due to premature commit. (spec.md §6 F3)
 *
 * V1 asserted 0 events (an offset planted past the record made the V1 auto-commit
 * consumer skip it). V2 asserts 1 event: the V2 consumer uses
 * {@code enable.auto.commit=false} and only calls {@code commitSync} after the DB
 * transaction has committed. There is no V1-style race.
 *
 * Test mechanism: send 1 order, run V2 consumer + relay, expect 1 event. The V1
 * "plant a bad offset" trick from the V1 test no longer reproduces a meaningful
 * failure in V2 — the bug class doesn't exist.
 */
@Tag("chaos")
class F3_LostOrdersTest extends KafkaTestFixture {

    @Test
    void v2CommitAfterWorkPreventsLoss() throws Exception {
        OrderProducer producer = new OrderProducer(bootstrap(), orderTopic);
        try (V2Stack stack = new V2Stack(bootstrap(), orderTopic, eventTopic, dlqTopic,
                groupId, Map.of(), dataSource())) {
            stack.start();

            Order order = new Order(
                    UUID.randomUUID().toString(),
                    "cust-f3",
                    List.of(new OrderItem("SKU-A", 1, new BigDecimal("9.99"))),
                    new BigDecimal("9.99"),
                    Instant.now());
            producer.send(order);

            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .until(() -> !TopicTailer.drain(bootstrap(), eventTopic, OrderFulfilled.class,
                            Duration.ofMillis(500)).isEmpty());

            List<OrderFulfilled> events = TopicTailer.drain(bootstrap(), eventTopic,
                    OrderFulfilled.class, Duration.ofSeconds(2));
            assertEquals(1, events.size(),
                    "V2 fix: manual commitSync after DB tx → records never lost");
            assertEquals(order.orderId(), events.get(0).orderId());
        } finally {
            producer.close();
        }
    }
}
