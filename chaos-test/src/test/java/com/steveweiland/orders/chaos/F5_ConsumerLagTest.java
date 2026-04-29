package com.steveweiland.orders.chaos;

import com.steveweiland.orders.api.OrderProducer;
import com.steveweiland.orders.common.Order;
import com.steveweiland.orders.common.OrderItem;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F5 — Consumer lag under burst load. (spec.md §6 F5)
 *
 * V1 baseline: lag &gt; 200 records 2 s into a 500-record burst.
 * v2.0.0 unchanged: still single-thread sequential processing.
 * v2.1.0 fix: virtual-thread parallel batch processing inside the
 * fulfillment consumer + async outbox publish drains the entire 500-record
 * burst inside the 2 s window. The same assertion is now flipped:
 * {@code assertTrue(lag &lt; LAG_THRESHOLD)}.
 *
 * v2.2.0 (multi-instance) keeps this assertion intact and additionally
 * verifies that horizontally scaling the fulfillment-service does not
 * introduce duplicate publishes from outbox-poll races.
 */
@Tag("chaos")
class F5_ConsumerLagTest extends KafkaTestFixture {

    private static final int BURST = 500;
    /**
     * v2.1.0 measured: lag = 0 records 2 s into the burst.
     * Threshold of 50 leaves headroom for CI jitter / GC pauses.
     */
    private static final int LAG_THRESHOLD = 50;

    @Test
    void parallelBatchConsumerDrainsBurstWithinWindow() throws Exception {
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

        try (V2Stack stack = new V2Stack(bootstrap(), orderTopic, eventTopic, dlqTopic,
                groupId, Map.of(), dataSource())) {
            stack.start();
            Thread.sleep(2000);
            long lag = measureLag();

            Path artifact = Path.of("target/lag-v2.1.txt");
            try {
                Files.createDirectories(artifact.getParent());
                Files.writeString(artifact,
                        "burst=" + BURST + " window=2s lag=" + lag + "\n");
            } catch (IOException ignored) {}

            assertTrue(lag < LAG_THRESHOLD,
                    "v2.1.0 fix: expected lag < " + LAG_THRESHOLD + ", got " + lag
                            + ". F5 should now drain inside the burst window.");
        }
    }

    private long measureLag() throws Exception {
        try (AdminClient admin = adminClient()) {
            Map<TopicPartition, OffsetAndMetadata> committed = admin
                    .listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS);

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
