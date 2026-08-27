package com.steveweiland.orders.fulfillment;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FulfillmentConsumerDedupeTest {

    @Test
    void keepsLastRecordForIdenticalDuplicates() {
        ConsumerRecords<String, byte[]> batch = batchOf(Map.of(
                tp(0), List.of(rec(0, 5, "order-a", "same"), rec(0, 7, "order-a", "same"), rec(0, 8, "order-b", "x"))));

        List<ConsumerRecord<String, byte[]>> out = FulfillmentConsumer.dedupeByKey(batch);

        assertEquals(2, out.size());
        ConsumerRecord<String, byte[]> keptA = out.stream()
                .filter(r -> r.key().equals("order-a")).findFirst().orElseThrow();
        assertEquals(7, keptA.offset(), "the LAST duplicate wins so its committed offset covers the dropped one");
    }

    @Test
    void sameKeyDifferentPayloadIsNotDeduped() {
        // F4's shape: a poison record and a valid order deliberately share a
        // partitioning key. Different bytes are different records — the poison
        // one must still reach the DLQ.
        ConsumerRecords<String, byte[]> batch = batchOf(Map.of(
                tp(0), List.of(rec(0, 5, "order-a", "{not valid json"), rec(0, 6, "order-a", "{\"valid\":true}"))));

        assertEquals(2, FulfillmentConsumer.dedupeByKey(batch).size());
    }

    @Test
    void sameKeyInDifferentPartitionsIsNotDeduped() {
        // Dropping a record from a partition where nothing else is processed
        // would leave that partition's offset permanently uncommitted; the
        // per-order advisory lock serializes cross-partition duplicates instead.
        ConsumerRecords<String, byte[]> batch = batchOf(Map.of(
                tp(0), List.of(rec(0, 1, "order-a", "v")),
                tp(1), List.of(rec(1, 4, "order-a", "v"))));

        assertEquals(2, FulfillmentConsumer.dedupeByKey(batch).size());
    }

    @Test
    void nullKeyedRecordsAreNeverDeduped() {
        ConsumerRecords<String, byte[]> batch = batchOf(Map.of(
                tp(2), List.of(rec(2, 1, null, "poison-1"), rec(2, 2, null, "poison-2"))));

        List<ConsumerRecord<String, byte[]>> out = FulfillmentConsumer.dedupeByKey(batch);
        assertEquals(2, out.size());
        assertTrue(out.stream().allMatch(r -> r.key() == null));
    }

    private static TopicPartition tp(int partition) {
        return new TopicPartition("orders", partition);
    }

    private static ConsumerRecord<String, byte[]> rec(int partition, long offset, String key, String value) {
        return new ConsumerRecord<>("orders", partition, offset, key, value.getBytes(StandardCharsets.UTF_8));
    }

    private static ConsumerRecords<String, byte[]> batchOf(Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> records) {
        return new ConsumerRecords<>(records);
    }
}
