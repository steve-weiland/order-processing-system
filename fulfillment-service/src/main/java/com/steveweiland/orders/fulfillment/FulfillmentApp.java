package com.steveweiland.orders.fulfillment;

import com.steveweiland.orders.common.db.Db;
import com.steveweiland.orders.common.db.Migrations;
import com.steveweiland.orders.common.topics.TopicAdmin;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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

        TopicAdmin.ensure(bootstrap, List.of(
                new NewTopic("orders", 3, (short) 1),
                new NewTopic("order-events", 3, (short) 1),
                new NewTopic("orders.dlq", 3, (short) 1)));

        HikariDataSource ds = Db.pool(jdbcUrl, jdbcUser, jdbcPassword, "fulfillment-pool", 16);
        Migrations.migrate(ds);

        ProcessedOrdersStore processedStore = new ProcessedOrdersStore();
        OutboxStore outboxStore = new OutboxStore(ds);
        DlqProducer dlq = new DlqProducer(bootstrap);

        OutboxRelay relay = new OutboxRelay(bootstrap, outboxStore, pollMs);
        FulfillmentConsumer consumer = new FulfillmentConsumer(bootstrap, ds, processedStore, outboxStore, dlq);

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

        log.info("fulfillment-service starting bootstrap={} jdbc={} pollMs={}", bootstrap, jdbcUrl, pollMs);
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
