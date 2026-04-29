package com.steveweiland.orders.fulfillment;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Polls the {@code outbox} table for unpublished rows and publishes each to its
 * destination Kafka topic, then marks the row published.
 *
 * <p>v2.2.0 multi-instance semantics: the entire poll → publish → mark cycle
 * runs in a single Postgres transaction. {@code SELECT … FOR UPDATE SKIP LOCKED}
 * lets multiple relay instances run concurrently against the same DB without
 * picking up the same rows. The lock is held until {@code COMMIT} so concurrent
 * pollers cannot lock-and-publish a row this relay has already published but
 * not yet marked.
 *
 * <p>Within a session the producer is idempotent ({@code enable.idempotence=true});
 * cross-session retry collapse is handled by the consumer-side
 * {@code processed_orders} idempotency check.
 */
public final class OutboxRelay implements Runnable, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH = 100;

    private final DataSource ds;
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
        this.ds = store.dataSource();
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
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            try {
                List<OutboxStore.Pending> pending = store.findUnpublished(c, BATCH);
                if (pending.isEmpty()) {
                    c.commit();
                    return 0;
                }

                // Async batch: issue all sends, then await each future. The DB
                // transaction (and the row locks) stays open across the publish.
                List<Future<RecordMetadata>> futures = new ArrayList<>(pending.size());
                for (OutboxStore.Pending p : pending) {
                    futures.add(producer.send(new ProducerRecord<>(p.topic(), p.recordKey(), p.payload())));
                }
                for (Future<RecordMetadata> f : futures) {
                    f.get(10, TimeUnit.SECONDS);
                }

                List<Long> ids = new ArrayList<>(pending.size());
                for (OutboxStore.Pending p : pending) ids.add(p.id());
                store.markPublishedBatch(c, ids);

                c.commit();   // releases the row locks
                log.info("relay published count={}", pending.size());
                return pending.size();
            } catch (Exception e) {
                c.rollback();
                throw e;
            }
        }
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
