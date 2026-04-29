package com.steveweiland.orders.chaos;

import com.steveweiland.orders.api.OrderProducer;
import com.steveweiland.orders.common.Order;
import com.steveweiland.orders.common.OrderFulfilled;
import com.steveweiland.orders.common.OrderItem;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F4 — Poison message. (spec.md §6 F4)
 *
 * V1: deserialization exception killed the consumer thread; valid record
 * behind it stalled (asserted 0 valid events + consumer crashed).
 *
 * V2: consumer reads as {@code byte[]}, parses JSON manually; on parse
 * failure, the raw bytes go to {@code orders.dlq} with diagnostic headers
 * and the source offset is committed. The valid record behind the poison
 * is processed normally.
 */
@Tag("chaos")
class F4_PoisonMessageTest extends KafkaTestFixture {

    @Test
    void poisonMessageRoutedToDlqAndConsumerContinues() throws Exception {
        // Use a real UUID for the partitioning key so the valid order behind the
        // poison record (which uses the same key for same-partition placement) has
        // a UUID-shaped orderId — the schema's order_id column requires UUID.
        String poisonKey = UUID.randomUUID().toString();
        byte[] poison = "{not valid json".getBytes(StandardCharsets.UTF_8);

        Properties rawProps = new Properties();
        rawProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        rawProps.put(ProducerConfig.ACKS_CONFIG, "1");
        try (KafkaProducer<String, byte[]> raw = new KafkaProducer<>(rawProps,
                new StringSerializer(), new ByteArraySerializer())) {
            raw.send(new ProducerRecord<>(orderTopic, poisonKey, poison)).get();
        }

        OrderProducer producer = new OrderProducer(bootstrap(), orderTopic);
        Order valid = new Order(
                poisonKey,
                "cust-f4",
                List.of(new OrderItem("SKU-A", 1, new BigDecimal("9.99"))),
                new BigDecimal("9.99"),
                Instant.now());
        producer.send(valid);

        try (V2Stack stack = new V2Stack(bootstrap(), orderTopic, eventTopic, dlqTopic,
                groupId, Map.of(), dataSource())) {
            stack.start();

            // Wait for the valid record's event to surface.
            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .until(() -> !TopicTailer.drain(bootstrap(), eventTopic, OrderFulfilled.class,
                            Duration.ofMillis(500)).isEmpty());

            List<OrderFulfilled> events = TopicTailer.drain(bootstrap(), eventTopic,
                    OrderFulfilled.class, Duration.ofSeconds(2));
            List<DlqRecord> dlq = drainDlq();

            assertFalse(stack.consumer.crashed(),
                    "V2 fix: consumer no longer crashes on poison");
            assertEquals(1, events.size(),
                    "V2 fix: valid order behind poison is fulfilled normally");
            assertEquals(valid.orderId(), events.get(0).orderId());
            assertEquals(1, dlq.size(),
                    "V2 fix: poison record routed to DLQ");
            DlqRecord d = dlq.get(0);
            assertTrue(d.headers.containsKey("x-dlq-reason"), "DLQ record missing x-dlq-reason");
            assertEquals(orderTopic, d.headers.get("x-dlq-source-topic"),
                    "DLQ record missing or wrong x-dlq-source-topic");
            assertTrue(d.headers.containsKey("x-dlq-source-partition"));
            assertTrue(d.headers.containsKey("x-dlq-source-offset"));
        } finally {
            producer.close();
        }
    }

    private List<DlqRecord> drainDlq() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-tail-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        List<DlqRecord> out = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(props,
                new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            c.subscribe(List.of(dlqTopic));
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            int emptyPolls = 0;
            while (System.nanoTime() < deadline) {
                ConsumerRecords<byte[], byte[]> batch = c.poll(Duration.ofMillis(200));
                if (batch.isEmpty()) {
                    if (++emptyPolls >= 3 && !out.isEmpty()) break;
                    continue;
                }
                for (ConsumerRecord<byte[], byte[]> rec : batch) {
                    var hMap = new java.util.HashMap<String, String>();
                    for (Header h : rec.headers()) {
                        hMap.put(h.key(), new String(h.value(), StandardCharsets.UTF_8));
                    }
                    out.add(new DlqRecord(rec.key(), rec.value(), hMap));
                }
            }
        }
        return out;
    }

    private record DlqRecord(byte[] key, byte[] value, Map<String, String> headers) {}
}
