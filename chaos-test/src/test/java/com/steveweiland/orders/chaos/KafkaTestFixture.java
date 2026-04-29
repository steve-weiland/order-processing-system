package com.steveweiland.orders.chaos;

import com.steveweiland.orders.common.db.Db;
import com.steveweiland.orders.common.db.Migrations;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class KafkaTestFixture {
    protected static final KafkaContainer KAFKA;
    protected static final PostgreSQLContainer<?> POSTGRES;
    protected static final HikariDataSource DS;

    static {
        KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));
        POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("orders").withUsername("orders").withPassword("orders");
        KAFKA.start();
        POSTGRES.start();
        DS = Db.pool(POSTGRES.getJdbcUrl(), "orders", "orders", "chaos-pool", 8);
        Migrations.migrate(DS);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { DS.close(); } catch (Exception ignored) {}
            POSTGRES.stop();
            KAFKA.stop();
        }));
    }

    protected String orderTopic;
    protected String eventTopic;
    protected String dlqTopic;
    protected String groupId;

    protected static String bootstrap() {
        return KAFKA.getBootstrapServers();
    }

    @BeforeEach
    final void freshTopicsAndDb() throws ExecutionException, InterruptedException, TimeoutException {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        orderTopic = "orders-" + suffix;
        eventTopic = "order-events-" + suffix;
        dlqTopic = "orders-dlq-" + suffix;
        groupId = "fulfillment-" + suffix;

        try (AdminClient admin = adminClient()) {
            admin.createTopics(List.of(
                    new NewTopic(orderTopic, 3, (short) 1),
                    new NewTopic(eventTopic, 3, (short) 1),
                    new NewTopic(dlqTopic, 3, (short) 1)
            )).all().get(10, TimeUnit.SECONDS);
        }

        // Per-test isolation for DB state. Tests use unique orderIds anyway, but
        // truncating keeps row counts predictable for diagnostics.
        try (Connection c = DS.getConnection(); Statement s = c.createStatement()) {
            s.execute("TRUNCATE processed_orders, outbox, idempotency_keys, processed_notifications, sagas");
        } catch (Exception e) {
            throw new RuntimeException("DB cleanup failed", e);
        }
    }

    @AfterEach
    final void deleteTopics() {
        try (AdminClient admin = adminClient()) {
            admin.deleteTopics(List.of(orderTopic, eventTopic, dlqTopic)).all().get(10, TimeUnit.SECONDS);
        } catch (Exception ignored) {}
    }

    @AfterAll
    final void afterAll() {
        // shared resources stopped by JVM shutdown hook
    }

    protected AdminClient adminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        return AdminClient.create(props);
    }

    protected DataSource dataSource() {
        return DS;
    }
}
