package com.steveweiland.orders.chaos;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class KafkaTestFixture {
    protected static final KafkaContainer KAFKA;

    static {
        KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));
        KAFKA.start();
        Runtime.getRuntime().addShutdownHook(new Thread(KAFKA::stop));
    }

    protected String orderTopic;
    protected String eventTopic;
    protected String groupId;

    protected static String bootstrap() {
        return KAFKA.getBootstrapServers();
    }

    @BeforeAll
    final void warmup() {
        // No-op — KAFKA is a class-loaded singleton; this triggers <clinit>.
    }

    @BeforeEach
    final void freshTopics() throws ExecutionException, InterruptedException, TimeoutException {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        orderTopic = "orders-" + suffix;
        eventTopic = "order-events-" + suffix;
        groupId = "fulfillment-" + suffix;

        try (AdminClient admin = adminClient()) {
            admin.createTopics(List.of(
                    new NewTopic(orderTopic, 3, (short) 1),
                    new NewTopic(eventTopic, 3, (short) 1)
            )).all().get(10, TimeUnit.SECONDS);
        }
    }

    @AfterEach
    final void deleteTopics() throws Exception {
        try (AdminClient admin = adminClient()) {
            admin.deleteTopics(List.of(orderTopic, eventTopic)).all().get(10, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // best-effort cleanup
        }
    }

    protected AdminClient adminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        return AdminClient.create(props);
    }
}
