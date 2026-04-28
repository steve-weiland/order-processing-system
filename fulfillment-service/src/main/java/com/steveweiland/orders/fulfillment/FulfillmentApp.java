package com.steveweiland.orders.fulfillment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FulfillmentApp {
    private static final Logger log = LoggerFactory.getLogger(FulfillmentApp.class);

    private FulfillmentApp() {}

    public static void main(String[] args) {
        String bootstrap = strFlag(args, "--bootstrap-servers",
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));

        FulfillmentStore store = new FulfillmentStore();
        EventProducer events = new EventProducer(bootstrap);
        FulfillmentConsumer consumer = new FulfillmentConsumer(bootstrap, events, store);

        Thread worker = new Thread(consumer, "fulfillment-consumer");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down");
            consumer.stop();
            try {
                worker.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            events.close();
        }, "fulfillment-shutdown"));

        log.info("fulfillment-service starting bootstrap={}", bootstrap);
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
