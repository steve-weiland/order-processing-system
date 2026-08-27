package com.steveweiland.orders.chaos;

import com.steveweiland.orders.common.JsonSerializer;
import com.steveweiland.orders.common.OrderFulfilled;
import com.steveweiland.orders.common.dlq.DlqProducer;
import com.steveweiland.orders.notification.NotificationConsumer;
import com.steveweiland.orders.notification.ProcessedNotificationsStore;
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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F15 — Poison message on order-events stalls notification-service. (spec.md §6 F15)
 *
 * The F4 lesson — never deserialize inside the Kafka deserializer — was
 * applied only to fulfillment-service. notification-service kept a typed
 * JsonDeserializer wired into the consumer, so one malformed record on
 * order-events threw out of poll(), killed the consumer thread, and stalled
 * the partition on every restart. There was no DLQ for order-events. (The F7
 * manual demo pipes hand-typed JSON into order-events — one typo away from
 * wedging the service.)
 *
 * The fix under test: deserialize as byte[], parse manually, route parse
 * failures to order-events.dlq with the standard x-dlq-* headers, commit the
 * source offset, continue.
 */
@Tag("chaos")
class F15_NotificationPoisonMessageTest extends KafkaTestFixture {

    @Test
    void poisonEventGoesToDlqAndConsumerSurvives() throws Exception {
        String notifGroup = "notif-" + UUID.randomUUID().toString().substring(0, 8);
        ProcessedNotificationsStore store = new ProcessedNotificationsStore(dataSource());
        NotificationConsumer consumer = new NotificationConsumer(
                bootstrap(), eventTopic, notifGroup, Map.of(), store,
                new DlqProducer(bootstrap(), eventsDlqTopic));

        LinkedBlockingQueue<OrderFulfilled> notified = new LinkedBlockingQueue<>();
        consumer.setHook(notified::offer);

        Thread worker = new Thread(consumer, "notif-f15");
        worker.start();
        try {
            // Poison first, then a valid event with the SAME key → same
            // partition, ordered behind the poison. Pre-fix the consumer died
            // on the poison and the valid event was never notified.
            String orderId = UUID.randomUUID().toString();
            byte[] poison = "{not valid json".getBytes(StandardCharsets.UTF_8);
            Properties rawProps = new Properties();
            rawProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
            try (KafkaProducer<String, byte[]> raw = new KafkaProducer<>(
                    rawProps, new StringSerializer(), new ByteArraySerializer())) {
                raw.send(new ProducerRecord<>(eventTopic, orderId, poison)).get();
            }
            OrderFulfilled valid = new OrderFulfilled(orderId, "cust-f15", Instant.now());
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
            try (KafkaProducer<String, OrderFulfilled> p = new KafkaProducer<>(
                    props, new StringSerializer(), new JsonSerializer<>())) {
                p.send(new ProducerRecord<>(eventTopic, orderId, valid)).get();
            }

            OrderFulfilled seen = notified.poll(15, TimeUnit.SECONDS);
            assertNotNull(seen, "REGRESSION GUARD: pre-fix the consumer crashed on the poison " +
                    "and the valid event behind it was never notified");
            assertEquals(orderId, seen.orderId());
            assertFalse(consumer.crashed(), "consumer survived the poison record");

            List<DlqRecord> dlqRecords = drainDlq();
            assertEquals(1, dlqRecords.size(), "poison routed to order-events.dlq");
            DlqRecord d = dlqRecords.get(0);
            assertEquals("{not valid json", new String(d.value(), StandardCharsets.UTF_8));
            assertTrue(d.headers().containsKey("x-dlq-reason"));
            assertEquals(eventTopic, d.headers().get("x-dlq-source-topic"));
        } finally {
            consumer.stop();
            worker.join(10_000);
            consumer.close();
        }
    }

    private List<DlqRecord> drainDlq() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-tailer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        List<DlqRecord> out = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(props,
                new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            c.subscribe(List.of(eventsDlqTopic));
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            int emptyPolls = 0;
            while (System.nanoTime() < deadline) {
                ConsumerRecords<byte[], byte[]> batch = c.poll(Duration.ofMillis(200));
                if (batch.isEmpty()) {
                    if (++emptyPolls >= 3 && !out.isEmpty()) break;
                    continue;
                }
                emptyPolls = 0;
                for (ConsumerRecord<byte[], byte[]> rec : batch) {
                    Map<String, String> headers = new HashMap<>();
                    for (Header h : rec.headers()) {
                        headers.put(h.key(), new String(h.value(), StandardCharsets.UTF_8));
                    }
                    out.add(new DlqRecord(rec.value(), headers));
                }
            }
        }
        return out;
    }

    private record DlqRecord(byte[] value, Map<String, String> headers) {}
}
