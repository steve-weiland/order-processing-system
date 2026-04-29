package com.steveweiland.orders.fulfillment;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Polls the {@code outbox} table for unpublished rows and publishes each to its
 * destination Kafka topic, then marks the row published. Runs in a dedicated
 * thread inside the fulfillment-service JVM.
 *
 * Semantics: at-least-once. If the process crashes between successful publish
 * and the {@code UPDATE outbox SET published_at} that follows, the next loop
 * republishes — duplicates downstream are absorbed by the consumer-side
 * idempotency check in {@link FulfillmentConsumer}. The Kafka producer is
 * configured with {@code enable.idempotence=true} so retries within a single
 * session don't multiply records on the broker.
 */
public final class OutboxRelay implements Runnable, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH = 100;

    private final OutboxStore store;
    private final KafkaProducer<String, byte[]> producer;
    private final long pollIntervalMs;
    private volatile boolean running = true;

    public OutboxRelay(String bootstrapServers, OutboxStore store, long pollIntervalMs) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "outbox-relay");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        this.producer = new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer());
        this.store = store;
        this.pollIntervalMs = pollIntervalMs;
    }

    @Override
    public void run() {
        log.info("outbox relay started pollInterval={}ms", pollIntervalMs);
        while (running) {
            try {
                int published = pollAndPublish();
                if (published == 0) {
                    Thread.sleep(pollIntervalMs);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("relay tick failed; will retry", e);
                try { Thread.sleep(pollIntervalMs); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt(); break;
                }
            }
        }
        // Drain anything that arrived between running=false and exit.
        try { pollAndPublish(); } catch (Exception e) {
            log.warn("final drain failed", e);
        }
        log.info("outbox relay stopped");
    }

    /** Visible for tests so they can deterministically push the relay forward. */
    public int pollAndPublish() throws Exception {
        List<OutboxStore.Pending> pending = store.findUnpublished(BATCH);
        for (OutboxStore.Pending p : pending) {
            ProducerRecord<String, byte[]> rec = new ProducerRecord<>(p.topic(), p.recordKey(), p.payload());
            producer.send(rec).get(10, TimeUnit.SECONDS);
            store.markPublished(p.id());
        }
        if (!pending.isEmpty()) log.info("relay published count={}", pending.size());
        return pending.size();
    }

    public void stop() {
        running = false;
    }

    @Override
    public void close() {
        stop();
        producer.close(Duration.ofSeconds(10));
    }
}
