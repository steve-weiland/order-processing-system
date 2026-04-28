package com.steveweiland.orders.chaos;

import com.steveweiland.orders.api.OrderProducer;
import com.steveweiland.orders.common.Order;
import com.steveweiland.orders.common.OrderFulfilled;
import com.steveweiland.orders.common.OrderItem;
import com.steveweiland.orders.fulfillment.EventProducer;
import com.steveweiland.orders.fulfillment.FulfillmentConsumer;
import com.steveweiland.orders.fulfillment.FulfillmentStore;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F4 — Poison message stalls the partition (spec.md §6 F4)
 *
 * V1 baseline: PASSES by asserting the bug.
 * Mechanism: write a malformed JSON byte string to a single partition of the
 * orders topic, then write a valid order to the SAME partition (using the
 * same key so partitioning is deterministic). The consumer hits the poison
 * record first, throws SerializationException out of poll(), and terminates.
 * The valid record is never processed.
 *
 * V2 expectation: with a DLQ, the poison record is routed to `orders.dlq`
 * after N retries and the consumer skips past it. assertEquals 1 valid
 * fulfillment event, and the consumer is still alive.
 */
@Tag("chaos")
class F4_PoisonMessageTest extends KafkaTestFixture {

    @Test
    void malformedJsonCrashesConsumerAndStallsPartition() throws Exception {
        // Step 1: write the poison record on a deterministic key.
        String poisonKey = "poison-key";
        byte[] poison = "{not valid json".getBytes(StandardCharsets.UTF_8);

        Properties rawProps = new Properties();
        rawProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        rawProps.put(ProducerConfig.ACKS_CONFIG, "1");
        try (KafkaProducer<String, byte[]> raw = new KafkaProducer<>(rawProps,
                new StringSerializer(), new ByteArraySerializer())) {
            raw.send(new ProducerRecord<>(orderTopic, poisonKey, poison)).get();
        }

        // Step 2: write a valid order on the SAME key — same partition, after the poison.
        OrderProducer producer = new OrderProducer(bootstrap(), orderTopic);
        Order valid = new Order(
                poisonKey,
                "cust-f4",
                List.of(new OrderItem("SKU-A", 1, new BigDecimal("9.99"))),
                new BigDecimal("9.99"),
                Instant.now());
        producer.send(valid);

        // Step 3: start the consumer. It will poll, hit the poison record, and crash.
        EventProducer eventProducer = new EventProducer(bootstrap(), eventTopic);
        FulfillmentStore store = new FulfillmentStore();
        FulfillmentConsumer consumer = new FulfillmentConsumer(
                bootstrap(), orderTopic, groupId, Map.of(), eventProducer, store);

        Thread worker = new Thread(consumer, "fulfillment-f4");
        worker.setUncaughtExceptionHandler((t, e) -> { /* expected — swallow */ });
        worker.start();

        try {
            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(200))
                    .until(consumer::crashed);

            // The consumer thread terminated; the valid record was never processed.
            List<OrderFulfilled> events = TopicTailer.drain(bootstrap(), eventTopic,
                    OrderFulfilled.class, Duration.ofSeconds(2));

            assertTrue(consumer.crashed(),
                    "V1 baseline: consumer must terminate on poison message");
            assertEquals(0, events.size(),
                    "V1 baseline: valid order behind the poison record is never fulfilled");
        } finally {
            consumer.stop();
            worker.join(10_000);
            producer.close();
            eventProducer.close();
        }
    }
}
