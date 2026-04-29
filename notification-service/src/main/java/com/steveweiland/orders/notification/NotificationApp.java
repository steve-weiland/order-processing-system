package com.steveweiland.orders.notification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class NotificationApp {
    private static final Logger log = LoggerFactory.getLogger(NotificationApp.class);

    private NotificationApp() {}

    public static void main(String[] args) throws Exception {
        String bootstrap = strFlag(args, "--bootstrap-servers",
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));

        com.steveweiland.orders.common.topics.TopicAdmin.ensure(bootstrap, java.util.List.of(
                new org.apache.kafka.clients.admin.NewTopic("order-events", 3, (short) 1)));

        NotificationConsumer consumer = new NotificationConsumer(bootstrap);
        Thread worker = new Thread(consumer, "notification-consumer");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down");
            consumer.stop();
            try {
                worker.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "notification-shutdown"));

        log.info("notification-service starting bootstrap={}", bootstrap);
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
