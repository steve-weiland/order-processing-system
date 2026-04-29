package com.steveweiland.orders.common.topics;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class TopicAdmin {
    private static final Logger log = LoggerFactory.getLogger(TopicAdmin.class);

    private TopicAdmin() {}

    /**
     * Create the given topics if they don't already exist. Idempotent — safe to
     * call concurrently from every service at startup. Required because V2 sets
     * {@code auto.create.topics.enable=false}.
     */
    public static void ensure(String bootstrapServers, List<NewTopic> topics)
            throws InterruptedException, ExecutionException, TimeoutException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient admin = AdminClient.create(props)) {
            for (NewTopic t : topics) {
                try {
                    admin.createTopics(List.of(t)).all().get(15, TimeUnit.SECONDS);
                    log.info("created topic={} partitions={} rf={}", t.name(), t.numPartitions(), t.replicationFactor());
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof TopicExistsException) {
                        log.info("topic exists topic={}", t.name());
                    } else {
                        throw e;
                    }
                }
            }
        }
    }
}
