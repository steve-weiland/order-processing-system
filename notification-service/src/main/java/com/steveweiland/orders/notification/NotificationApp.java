package com.steveweiland.orders.notification;

import com.steveweiland.orders.common.db.Db;
import com.steveweiland.orders.common.db.Migrations;
import com.steveweiland.orders.common.topics.TopicAdmin;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class NotificationApp {
    private static final Logger log = LoggerFactory.getLogger(NotificationApp.class);

    private NotificationApp() {}

    public static void main(String[] args) throws Exception {
        String bootstrap = strFlag(args, "--bootstrap-servers",
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        String jdbcUrl = strFlag(args, "--jdbc-url",
                System.getenv().getOrDefault("JDBC_URL", "jdbc:postgresql://localhost:5432/orders"));
        String jdbcUser = strFlag(args, "--jdbc-user",
                System.getenv().getOrDefault("JDBC_USER", "orders"));
        String jdbcPassword = strFlag(args, "--jdbc-password",
                System.getenv().getOrDefault("JDBC_PASSWORD", "orders"));

        TopicAdmin.ensure(bootstrap, List.of(
                new NewTopic("order-events", 3, (short) 1)));

        HikariDataSource ds = Db.pool(jdbcUrl, jdbcUser, jdbcPassword, "notification-pool", 4);
        Migrations.migrate(ds);

        NotificationConsumer consumer = new NotificationConsumer(bootstrap, ds);
        Thread worker = new Thread(consumer, "notification-consumer");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down");
            consumer.stop();
            try {
                worker.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            ds.close();
        }, "notification-shutdown"));

        log.info("notification-service starting bootstrap={} jdbc={}", bootstrap, jdbcUrl);
        worker.start();
        try {
            worker.join();
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
