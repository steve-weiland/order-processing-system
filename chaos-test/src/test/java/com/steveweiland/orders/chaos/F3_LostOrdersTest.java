package com.steveweiland.orders.chaos;

import com.steveweiland.orders.api.OrderProducer;
import com.steveweiland.orders.common.Order;
import com.steveweiland.orders.common.OrderFulfilled;
import com.steveweiland.orders.common.OrderItem;
import com.steveweiland.orders.fulfillment.EventProducer;
import com.steveweiland.orders.fulfillment.FulfillmentConsumer;
import com.steveweiland.orders.fulfillment.FulfillmentStore;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
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
 * F3 — Lost orders due to premature commit (spec.md §6 F3)
 *
 * V1 baseline: PASSES by asserting the bug.
 * Mechanism: pre-commit an offset past the record using AdminClient. This
 * simulates the V1 race where auto-commit fires (offset advances) but the
 * consumer crashes before completing the work for that record. On restart,
 * the consumer skips the record entirely.
 *
 * V2 expectation: with manual commitSync only AFTER work completes, no commit
 * happens until the OrderFulfilled event is durably emitted. assertEquals 1.
 */
@Tag("chaos")
class F3_LostOrdersTest extends KafkaTestFixture {

    @Test
    void prematureOffsetCommitSkipsRecord() throws Exception {
        OrderProducer producer = new OrderProducer(bootstrap(), orderTopic);
        EventProducer eventProducer = new EventProducer(bootstrap(), eventTopic);

        Order order = new Order(
                UUID.randomUUID().toString(),
                "cust-f3",
                List.of(new OrderItem("SKU-A", 1, new BigDecimal("9.99"))),
                new BigDecimal("9.99"),
                Instant.now());
        producer.send(order);

        // Plant a "we already processed this" offset for the consumer group, BEFORE
        // any consumer of that group has read. This reproduces the post-crash state
        // where auto-commit advanced the offset but the work never completed.
        try (AdminClient admin = adminClient()) {
            Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
            for (int p = 0; p < 3; p++) latest.put(new TopicPartition(orderTopic, p), OffsetSpec.latest());
            ListOffsetsResult.ListOffsetsResultInfo info;
            Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
            for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> e
                    : admin.listOffsets(latest).all().get(10, TimeUnit.SECONDS).entrySet()) {
                commits.put(e.getKey(), new OffsetAndMetadata(e.getValue().offset()));
            }
            admin.alterConsumerGroupOffsets(groupId, commits).all().get(10, TimeUnit.SECONDS);
        }

        FulfillmentStore store = new FulfillmentStore();
        FulfillmentConsumer consumer = new FulfillmentConsumer(
                bootstrap(), orderTopic, groupId, Map.of(), eventProducer, store);

        Thread worker = new Thread(consumer, "fulfillment-f3");
        worker.start();
        try {
            // Give the consumer time to be assigned partitions and poll. If anything
            // were going to be processed, it would be by now.
            Thread.sleep(3000);

            List<OrderFulfilled> events = TopicTailer.drain(bootstrap(), eventTopic,
                    OrderFulfilled.class, Duration.ofSeconds(2));

            // V1: ZERO events because the offset was advanced past the unprocessed
            // record. V2 will assert 1.
            assertEquals(0, events.size(),
                    "V1 baseline: record skipped because offset was committed past it");
        } finally {
            consumer.stop();
            worker.join(10_000);
            producer.close();
            eventProducer.close();
        }
    }
}
