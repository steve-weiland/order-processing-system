package com.steveweiland.orders.chaos;

import com.steveweiland.orders.common.JsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Drains all currently-available records from a topic into an in-memory list.
 * Each call creates a fresh consumer with a unique group, so it does not
 * interfere with the system under test's consumer-group offsets.
 */
public final class TopicTailer {
    private TopicTailer() {}

    public static <V> List<V> drain(String bootstrap, String topic, Class<V> type, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "tailer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        List<V> out = new ArrayList<>();
        try (KafkaConsumer<String, V> consumer = new KafkaConsumer<>(props,
                new StringDeserializer(), new JsonDeserializer<>(type))) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + timeout.toNanos();
            int emptyPolls = 0;
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, V> batch = consumer.poll(Duration.ofMillis(200));
                if (batch.isEmpty()) {
                    if (++emptyPolls >= 3 && !out.isEmpty()) break;
                    continue;
                }
                emptyPolls = 0;
                for (ConsumerRecord<String, V> rec : batch) {
                    out.add(rec.value());
                }
            }
        }
        return out;
    }
}
