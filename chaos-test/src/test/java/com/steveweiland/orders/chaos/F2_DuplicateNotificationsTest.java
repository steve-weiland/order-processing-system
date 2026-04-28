package com.steveweiland.orders.chaos;

import com.steveweiland.orders.api.OrderProducer;
import com.steveweiland.orders.common.Order;
import com.steveweiland.orders.common.OrderFulfilled;
import com.steveweiland.orders.common.OrderItem;
import com.steveweiland.orders.fulfillment.EventProducer;
import com.steveweiland.orders.fulfillment.FulfillmentConsumer;
import com.steveweiland.orders.fulfillment.FulfillmentStore;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
 * F2 — Duplicate notifications after consumer crash (spec.md §6 F2)
 *
 * V1 baseline: PASSES by asserting the bug.
 * Mechanism: disable auto-commit on the first run so the offset is never
 * advanced. (Production V1 has auto-commit=true, but the BUG is the window
 * between work-completion and the next commit tick. Disabling the commit
 * deterministically reproduces the post-crash broker state — uncommitted
 * offset — without racing the 5 s tick.) On restart with the same group.id,
 * the original record is redelivered → second OrderFulfilled.
 *
 * V2 expectation: with a transactional outbox (state + offset committed
 * atomically), restart will not redeliver. assertEquals 1.
 */
@Tag("chaos")
class F2_DuplicateNotificationsTest extends KafkaTestFixture {

    @Test
    void consumerCrashBeforeCommitCausesDuplicateEvent() throws Exception {
        OrderProducer producer = new OrderProducer(bootstrap(), orderTopic);

        // Suppress all commits on the first run so the broker's __consumer_offsets
        // for this group stays at 0. KafkaConsumer.close() also commits when auto-commit
        // is enabled, so set it to false explicitly.
        Map<String, Object> noCommit = Map.of(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        FulfillmentStore store = new FulfillmentStore();
        EventProducer eventProducer1 = new EventProducer(bootstrap(), eventTopic);
        FulfillmentConsumer firstRun = new FulfillmentConsumer(
                bootstrap(), orderTopic, groupId, noCommit, eventProducer1, store);

        Thread worker1 = new Thread(firstRun, "fulfillment-run-1");

        Order order = new Order(
                UUID.randomUUID().toString(),
                "cust-f2",
                List.of(new OrderItem("SKU-A", 1, new BigDecimal("9.99"))),
                new BigDecimal("9.99"),
                Instant.now());
        producer.send(order);

        worker1.start();
        try {
            // Wait until the first fulfillment event lands.
            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .until(() -> TopicTailer.drain(bootstrap(), eventTopic, OrderFulfilled.class,
                            Duration.ofMillis(500)).size() >= 1);
        } finally {
            firstRun.stop();
            worker1.join(10_000);
            eventProducer1.close();
        }

        // Restart with the SAME group.id. Since auto-commit never fired, the consumer
        // resumes from the previous (zero) offset and redelivers the same record.
        EventProducer eventProducer2 = new EventProducer(bootstrap(), eventTopic);
        FulfillmentConsumer secondRun = new FulfillmentConsumer(
                bootstrap(), orderTopic, groupId, Map.of(), eventProducer2, store);

        Thread worker2 = new Thread(secondRun, "fulfillment-run-2");
        worker2.start();
        try {
            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .until(() -> TopicTailer.drain(bootstrap(), eventTopic, OrderFulfilled.class,
                            Duration.ofMillis(500)).size() >= 2);

            List<OrderFulfilled> events = TopicTailer.drain(bootstrap(), eventTopic,
                    OrderFulfilled.class, Duration.ofSeconds(2));

            // V1: TWO events for ONE logical order due to redelivery. V2 will assert 1.
            assertEquals(2, events.size(),
                    "V1 baseline: redelivery causes a second OrderFulfilled event");
        } finally {
            secondRun.stop();
            worker2.join(10_000);
            eventProducer2.close();
            producer.close();
        }
    }
}
