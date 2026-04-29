package com.steveweiland.orders.chaos;

import com.steveweiland.orders.api.OrderProducer;
import com.steveweiland.orders.common.Order;
import com.steveweiland.orders.common.OrderItem;
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
 * F5 — Consumer lag under burst load. (spec.md §6 F5)
 *
 * V2 does NOT address F5. The same V1-baseline assertion (lag &gt; 200) is
 * expected to hold in V2 — the consumer still does 50 ms of work per record
 * and runs single-threaded per partition. V3 will introduce parallel consumers
 * or {@code max.poll.records} tuning to make this lag go away.
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

        try (V2Stack stack = new V2Stack(bootstrap(), orderTopic, eventTopic, dlqTopic,
                groupId, Map.of(), dataSource())) {
            stack.start();
            Thread.sleep(2000);
            long lag = measureLag();

            Path artifact = Path.of("target/lag-v2.txt");
            try {
                Files.createDirectories(artifact.getParent());
                Files.writeString(artifact,
                        "burst=" + BURST + " window=2s lag=" + lag + "\n");
            } catch (IOException ignored) {}

            assertTrue(lag > LAG_THRESHOLD,
                    "V2 keeps the V1 baseline assertion: expected lag > " + LAG_THRESHOLD
                            + ", got " + lag + ". V3 will invert this.");
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
