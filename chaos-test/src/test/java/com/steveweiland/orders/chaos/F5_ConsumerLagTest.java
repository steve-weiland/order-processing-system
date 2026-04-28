package com.steveweiland.orders.chaos;

import com.steveweiland.orders.api.OrderProducer;
import com.steveweiland.orders.common.Order;
import com.steveweiland.orders.common.OrderItem;
import com.steveweiland.orders.fulfillment.EventProducer;
import com.steveweiland.orders.fulfillment.FulfillmentConsumer;
import com.steveweiland.orders.fulfillment.FulfillmentStore;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F5 — Consumer lag under burst load (spec.md §6 F5)
 *
 * V1 baseline: PASSES by asserting the bug.
 * A single consumer with 50 ms work simulation cannot keep up with a burst of
 * 500 records. After 2 s of processing, lag should be hundreds of records.
 *
 * V2 expectation: with parallel-consumer or tuned max.poll.records + threading,
 * the same 500 records drain to ~0 lag within the same window.
 *
 * This test also writes target/lag-v1.txt as a portfolio artifact (the actual
 * lag number, not just pass/fail). V2 sets a tighter bound based on this baseline.
 */
@Tag("chaos")
class F5_ConsumerLagTest extends KafkaTestFixture {

    private static final int BURST = 500;
    private static final int LAG_THRESHOLD = 200;

    @Test
    void slowConsumerBuildsLagUnderBurst() throws Exception {
        OrderProducer producer = new OrderProducer(bootstrap(), orderTopic);
        for (int i = 0; i < BURST; i++) {
            Order order = new Order(
                    UUID.randomUUID().toString(),
                    "cust-f5-" + i,
                    List.of(new OrderItem("SKU-A", 1, new BigDecimal("1.00"))),
                    new BigDecimal("1.00"),
                    Instant.now());
            producer.send(order);
        }
        producer.close();

        FulfillmentStore store = new FulfillmentStore();
        EventProducer eventProducer = new EventProducer(bootstrap(), eventTopic);
        FulfillmentConsumer consumer = new FulfillmentConsumer(
                bootstrap(), orderTopic, groupId, Map.of(), eventProducer, store);

        Thread worker = new Thread(consumer, "fulfillment-f5");
        worker.start();
        try {
            Thread.sleep(2000);
            long lag = measureLag();

            Path artifact = Path.of("target/lag-v1.txt");
            try {
                Files.createDirectories(artifact.getParent());
                Files.writeString(artifact,
                        "burst=" + BURST + " window=2s lag=" + lag + " store=" + store.size() + "\n");
            } catch (IOException ignored) {}

            assertTrue(lag > LAG_THRESHOLD,
                    "V1 baseline: expected lag > " + LAG_THRESHOLD + ", got " + lag
                            + " (store=" + store.size() + ")");
        } finally {
            consumer.stop();
            worker.join(15_000);
            eventProducer.close();
        }
    }

    private long measureLag() throws Exception {
        try (AdminClient admin = adminClient()) {
            // Committed offsets for this group.
            Map<TopicPartition, OffsetAndMetadata> committed = admin
                    .listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS);

            // End offsets across the partitions of orderTopic.
            Map<TopicPartition, OffsetSpec> req = new HashMap<>();
            for (int p = 0; p < 3; p++) req.put(new TopicPartition(orderTopic, p), OffsetSpec.latest());
            Map<TopicPartition, ListOffsetsResultInfo> end = admin.listOffsets(req).all().get(10, TimeUnit.SECONDS);

            long total = 0;
            for (Entry<TopicPartition, ListOffsetsResultInfo> e : end.entrySet()) {
                long endOffset = e.getValue().offset();
                long c = committed.containsKey(e.getKey()) ? committed.get(e.getKey()).offset() : 0L;
                total += Math.max(0, endOffset - c);
            }
            return total;
        }
    }
}
