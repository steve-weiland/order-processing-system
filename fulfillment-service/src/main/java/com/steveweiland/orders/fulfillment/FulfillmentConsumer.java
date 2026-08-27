package com.steveweiland.orders.fulfillment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.steveweiland.orders.common.JsonMapper;
import com.steveweiland.orders.common.dlq.DlqProducer;
import com.steveweiland.orders.common.Order;
import com.steveweiland.orders.common.OrderFulfilled;
import com.steveweiland.orders.fulfillment.saga.SagaOrchestrator;
import com.steveweiland.orders.fulfillment.saga.SagaResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

public final class FulfillmentConsumer implements Runnable, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(FulfillmentConsumer.class);
    public static final String DEFAULT_TOPIC = "orders";
    public static final String DEFAULT_EVENTS_TOPIC = "order-events";
    public static final String DEFAULT_GROUP_ID = "fulfillment";
    public static final int DEFAULT_WORKER_POOL_SIZE = 32;

    private final KafkaConsumer<String, byte[]> consumer;
    private final ObjectMapper mapper = JsonMapper.shared();
    private final DataSource ds;
    private final ProcessedOrdersStore processedStore;
    private final OutboxStore outboxStore;
    private final DlqProducer dlq;
    private final SagaOrchestrator orchestrator;
    private final String topic;
    private final String eventsTopic;
    private final ExecutorService workers;
    private final Semaphore concurrency;
    private volatile boolean running = true;
    private volatile Throwable terminalError;

    public FulfillmentConsumer(String bootstrapServers,
                               DataSource ds,
                               ProcessedOrdersStore processedStore,
                               OutboxStore outboxStore,
                               DlqProducer dlq,
                               SagaOrchestrator orchestrator) {
        this(bootstrapServers, DEFAULT_TOPIC, DEFAULT_EVENTS_TOPIC, DEFAULT_GROUP_ID,
                DEFAULT_WORKER_POOL_SIZE, Map.of(),
                ds, processedStore, outboxStore, dlq, orchestrator);
    }

    public FulfillmentConsumer(String bootstrapServers,
                               String topic,
                               String eventsTopic,
                               String groupId,
                               int workerPoolSize,
                               Map<String, Object> overrides,
                               DataSource ds,
                               ProcessedOrdersStore processedStore,
                               OutboxStore outboxStore,
                               DlqProducer dlq,
                               SagaOrchestrator orchestrator) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "fulfillment-service");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 200);
        props.putAll(overrides);

        this.consumer = new KafkaConsumer<>(props, new StringDeserializer(), new ByteArrayDeserializer());
        this.topic = topic;
        this.eventsTopic = eventsTopic;
        this.ds = ds;
        this.processedStore = processedStore;
        this.outboxStore = outboxStore;
        this.dlq = dlq;
        this.orchestrator = orchestrator;
        this.workers = Executors.newVirtualThreadPerTaskExecutor();
        this.concurrency = new Semaphore(Math.max(1, workerPoolSize));
    }

    @Override
    public void run() {
        consumer.subscribe(List.of(topic));
        log.info("subscribed topic={} workerPoolSize={}", topic, concurrency.availablePermits());
        try {
            while (running) {
                ConsumerRecords<String, byte[]> batch = consumer.poll(Duration.ofMillis(1000));
                if (batch.isEmpty()) continue;
                processBatch(batch);
            }
        } catch (WakeupException e) {
            if (running) throw e;
        } catch (Throwable t) {
            terminalError = t;
            log.error("consumer terminated by error", t);
            throw t;
        } finally {
            consumer.close(Duration.ofSeconds(10));
            workers.close();
            log.info("consumer closed");
        }
    }

    private void processBatch(ConsumerRecords<String, byte[]> batch) {
        Map<TopicPartition, Long> maxOffset = new ConcurrentHashMap<>();
        List<Future<?>> tasks = new ArrayList<>();

        List<ConsumerRecord<String, byte[]>> records = dedupeByKey(batch);
        for (ConsumerRecord<String, byte[]> rec : records) {
            tasks.add(workers.submit(() -> {
                concurrency.acquireUninterruptibly();
                try {
                    processOne(rec);
                    maxOffset.merge(
                            new TopicPartition(rec.topic(), rec.partition()),
                            rec.offset(),
                            Math::max);
                } finally {
                    concurrency.release();
                }
            }));
        }

        boolean allOk = true;
        for (Future<?> f : tasks) {
            try {
                f.get();
            } catch (ExecutionException e) {
                allOk = false;
                log.error("worker failed; will redeliver after rebalance", e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        if (allOk && !maxOffset.isEmpty()) {
            Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
            maxOffset.forEach((tp, off) -> commits.put(tp, new OffsetAndMetadata(off + 1)));
            consumer.commitSync(commits);
        }
    }

    /**
     * Drop exact duplicates within a poll batch, keeping the LAST occurrence
     * per (partition, key, value-bytes). True duplicates (producer retries,
     * redelivery) are byte-identical; dispatching both onto the parallel
     * executor would race two saga runs for the same order. Keeping the last
     * occurrence means the committed offset (kept-offset + 1) also covers
     * every dropped duplicate in that partition.
     *
     * <p>Value bytes are part of the identity on purpose: same-key records
     * with DIFFERENT payloads are not duplicates (e.g. a poison record and a
     * valid order sharing a partitioning key -- F4) and every one must be
     * processed. Scoped per-partition because dropping a record from a
     * partition where nothing else is processed would leave that partition's
     * offset permanently uncommitted. Anything this filter can't prove
     * identical -- cross-partition duplicates, differing bytes -- is
     * serialized by the orchestrator's per-order advisory lock instead.
     * Null-key records are never deduped.
     */
    static List<ConsumerRecord<String, byte[]>> dedupeByKey(ConsumerRecords<String, byte[]> batch) {
        Map<String, List<ConsumerRecord<String, byte[]>>> perKey = new LinkedHashMap<>();
        List<ConsumerRecord<String, byte[]>> nullKeyed = new ArrayList<>();
        int total = 0;
        for (ConsumerRecord<String, byte[]> rec : batch) {
            total++;
            if (rec.key() == null) {
                nullKeyed.add(rec);
                continue;
            }
            List<ConsumerRecord<String, byte[]>> variants =
                    perKey.computeIfAbsent(rec.partition() + " " + rec.key(), k -> new ArrayList<>(1));
            int i = 0;
            for (; i < variants.size(); i++) {
                if (Arrays.equals(variants.get(i).value(), rec.value())) {
                    variants.set(i, rec);   // identical duplicate -- last one wins
                    break;
                }
            }
            if (i == variants.size()) {
                variants.add(rec);          // same key, different payload -- keep both
            }
        }
        List<ConsumerRecord<String, byte[]>> out = new ArrayList<>(total);
        perKey.values().forEach(out::addAll);
        out.addAll(nullKeyed);
        if (out.size() < total) {
            log.info("batch dedupe dropped {} duplicate record(s) of {}", total - out.size(), total);
        }
        return out;
    }

    private void processOne(ConsumerRecord<String, byte[]> rec) {
        Order order;
        try {
            order = mapper.readValue(rec.value(), Order.class);
        } catch (Exception parseError) {
            try {
                dlq.send(rec, parseError);
            } catch (Exception dlqError) {
                log.error("DLQ publish failed; will redeliver source record", dlqError);
                throw new RuntimeException("DLQ publish failed", dlqError);
            }
            return;
        }

        MDC.put("orderId", order.orderId());
        try {
            // OPS-101: idempotency gate BEFORE any fulfillment work. Redelivery
            // of an already-processed order must not wake the saga (whose steps
            // have side effects) — the terminal-state short-circuit inside the
            // orchestrator remains as defense in depth. The atomic
            // INSERT … ON CONFLICT below stays the authority for claiming.
            try (Connection pre = ds.getConnection()) {
                if (processedStore.exists(pre, order.orderId())) {
                    log.info("idempotent skip (pre-saga) — already processed");
                    return;
                }
            }

            log.info("starting saga customerId={} partition={} offset={}",
                    order.customerId(), rec.partition(), rec.offset());

            SagaResult result = orchestrator.run(order);

            try (Connection c = ds.getConnection()) {
                c.setAutoCommit(false);
                try {
                    boolean completed = result.isCompleted();
                    Instant fulfilledAt = completed ? Instant.now() : null;
                    boolean claimed = processedStore.insert(c, order.orderId(), order.customerId(),
                            completed ? ProcessedOrdersStore.Status.FULFILLED : ProcessedOrdersStore.Status.FAILED,
                            fulfilledAt);
                    if (!claimed) {
                        c.commit();
                        log.info("idempotent skip — already processed");
                        return;
                    }

                    if (completed) {
                        OrderFulfilled event = new OrderFulfilled(order.orderId(), order.customerId(), fulfilledAt);
                        byte[] payload = mapper.writeValueAsBytes(event);
                        outboxStore.insert(c, order.orderId(), eventsTopic, order.orderId(), payload);
                    }

                    c.commit();
                    if (result.isCompleted()) {
                        log.info("saga completed — outbox row enqueued");
                    } else {
                        log.info("saga failed step={} reason={} — no outbox row",
                                result.failureStep(), result.failureReason());
                    }
                } catch (Exception inner) {
                    c.rollback();
                    throw inner;
                }
            }
        } catch (Exception e) {
            log.error("fulfillment DB transaction failed; offset will not be committed", e);
            throw new RuntimeException(e);
        } finally {
            MDC.remove("orderId");
        }
    }

    public boolean crashed() {
        return terminalError != null;
    }

    public Throwable terminalError() {
        return terminalError;
    }

    public void stop() {
        running = false;
        consumer.wakeup();
    }

    @Override
    public void close() {
        stop();
    }
}
