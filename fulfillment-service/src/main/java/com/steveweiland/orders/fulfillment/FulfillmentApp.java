package com.steveweiland.orders.fulfillment;

import com.steveweiland.orders.common.db.Db;
import com.steveweiland.orders.common.dlq.DlqProducer;
import com.steveweiland.orders.common.db.Migrations;
import com.steveweiland.orders.common.topics.TopicAdmin;
import com.steveweiland.orders.fulfillment.saga.InventoryStep;
import com.steveweiland.orders.fulfillment.saga.PaymentStep;
import com.steveweiland.orders.fulfillment.saga.SagaOrchestrator;
import com.steveweiland.orders.fulfillment.saga.SagaStore;
import com.steveweiland.orders.fulfillment.saga.ShippingStep;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public final class FulfillmentApp {
    private static final Logger log = LoggerFactory.getLogger(FulfillmentApp.class);

    private FulfillmentApp() {}

    public static void main(String[] args) throws Exception {
        String bootstrap = strFlag(args, "--bootstrap-servers",
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        String jdbcUrl = strFlag(args, "--jdbc-url",
                System.getenv().getOrDefault("JDBC_URL", "jdbc:postgresql://localhost:5432/orders"));
        String jdbcUser = strFlag(args, "--jdbc-user",
                System.getenv().getOrDefault("JDBC_USER", "orders"));
        String jdbcPassword = strFlag(args, "--jdbc-password",
                System.getenv().getOrDefault("JDBC_PASSWORD", "orders"));
        long pollMs = Long.parseLong(strFlag(args, "--outbox-poll-ms",
                System.getenv().getOrDefault("OUTBOX_POLL_MS", "100")));
        int workerPoolSize = Integer.parseInt(strFlag(args, "--worker-pool-size",
                System.getenv().getOrDefault("WORKER_POOL_SIZE",
                        String.valueOf(FulfillmentConsumer.DEFAULT_WORKER_POOL_SIZE))));
        double paymentFailureRate = Double.parseDouble(
                System.getenv().getOrDefault("PAYMENT_FAILURE_RATE", "0.0"));
        double inventoryFailureRate = Double.parseDouble(
                System.getenv().getOrDefault("INVENTORY_FAILURE_RATE", "0.0"));
        double shippingFailureRate = Double.parseDouble(
                System.getenv().getOrDefault("SHIPPING_FAILURE_RATE", "0.0"));

        TopicAdmin.ensure(bootstrap, List.of(
                new NewTopic("orders", 3, (short) 1),
                new NewTopic("order-events", 3, (short) 1),
                new NewTopic("orders.dlq", 3, (short) 1)));

        // Pool must cover the worker semaphore: every in-flight saga holds one
        // connection for its whole run (per-order advisory lock session), plus
        // headroom for the outbox relay and the pre-saga idempotency checks. A
        // pool smaller than the semaphore turns connection wait into the real
        // concurrency bound (F5 lag regresses).
        HikariDataSource ds = Db.pool(jdbcUrl, jdbcUser, jdbcPassword, "fulfillment-pool", workerPoolSize + 4);
        Migrations.migrate(ds);

        ProcessedOrdersStore processedStore = new ProcessedOrdersStore();
        OutboxStore outboxStore = new OutboxStore(ds);
        DlqProducer dlq = new DlqProducer(bootstrap, "orders.dlq");

        SagaStore sagaStore = new SagaStore(ds);
        SagaOrchestrator orchestrator = new SagaOrchestrator(
                sagaStore,
                new PaymentStep(paymentFailureRate),
                new InventoryStep(inventoryFailureRate),
                new ShippingStep(shippingFailureRate));

        OutboxRelay relay = new OutboxRelay(bootstrap, outboxStore, pollMs);
        FulfillmentConsumer consumer = new FulfillmentConsumer(
                bootstrap,
                FulfillmentConsumer.DEFAULT_TOPIC,
                FulfillmentConsumer.DEFAULT_EVENTS_TOPIC,
                FulfillmentConsumer.DEFAULT_GROUP_ID,
                workerPoolSize,
                Map.of(),
                ds, processedStore, outboxStore, dlq, orchestrator);

        Thread relayWorker = new Thread(relay, "outbox-relay");
        Thread consumerWorker = new Thread(consumer, "fulfillment-consumer");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down");
            consumer.stop();
            try { consumerWorker.join(10_000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            relay.stop();
            try { relayWorker.join(10_000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            relay.close();
            dlq.close();
            ds.close();
        }, "fulfillment-shutdown"));

        log.info("fulfillment-service starting bootstrap={} jdbc={} pollMs={} workers={} " +
                        "saga.payment.failureRate={} saga.inventory.failureRate={} saga.shipping.failureRate={}",
                bootstrap, jdbcUrl, pollMs, workerPoolSize,
                paymentFailureRate, inventoryFailureRate, shippingFailureRate);
        relayWorker.start();
        consumerWorker.start();
        try {
            consumerWorker.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String strFlag(String[] args, String name, String def) {
        for (int i = 0; i < args.length - 1; i++) {
            if (name.equals(args[i])) return args[i + 1];
        }
        return def;
    }
}
