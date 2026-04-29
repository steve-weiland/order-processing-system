package com.steveweiland.orders.chaos;

import com.steveweiland.orders.fulfillment.DlqProducer;
import com.steveweiland.orders.fulfillment.FulfillmentConsumer;
import com.steveweiland.orders.fulfillment.OutboxRelay;
import com.steveweiland.orders.fulfillment.OutboxStore;
import com.steveweiland.orders.fulfillment.ProcessedOrdersStore;
import com.steveweiland.orders.fulfillment.saga.InventoryStep;
import com.steveweiland.orders.fulfillment.saga.PaymentStep;
import com.steveweiland.orders.fulfillment.saga.SagaOrchestrator;
import com.steveweiland.orders.fulfillment.saga.SagaStore;
import com.steveweiland.orders.fulfillment.saga.ShippingStep;

import javax.sql.DataSource;
import java.util.Map;

/**
 * Test-side V2 wiring: assembles ProcessedOrdersStore + OutboxStore + DlqProducer
 * + OutboxRelay + FulfillmentConsumer against the test's Postgres + Kafka.
 *
 * Construct one per test, start the relay and consumer threads, then call
 * {@link #close()} in a finally block.
 */
final class V2Stack implements AutoCloseable {
    final DlqProducer dlq;
    final OutboxStore outboxStore;
    final ProcessedOrdersStore processedStore;
    final OutboxRelay relay;
    final FulfillmentConsumer consumer;
    final SagaStore sagaStore;
    final PaymentStep payment;
    final InventoryStep inventory;
    final ShippingStep shipping;
    final SagaOrchestrator orchestrator;
    final Thread relayWorker;
    final Thread consumerWorker;

    V2Stack(String bootstrap,
            String orderTopic,
            String eventTopic,
            String dlqTopic,
            String groupId,
            Map<String, Object> consumerOverrides,
            DataSource ds) {
        this(bootstrap, orderTopic, eventTopic, dlqTopic, groupId, consumerOverrides, ds, 0.0, 0.0, 0.0);
    }

    V2Stack(String bootstrap,
            String orderTopic,
            String eventTopic,
            String dlqTopic,
            String groupId,
            Map<String, Object> consumerOverrides,
            DataSource ds,
            double paymentFailureRate,
            double inventoryFailureRate,
            double shippingFailureRate) {
        this.dlq = new DlqProducer(bootstrap, dlqTopic);
        this.outboxStore = new OutboxStore(ds);
        this.processedStore = new ProcessedOrdersStore();
        this.sagaStore = new SagaStore(ds);
        this.payment = new PaymentStep(paymentFailureRate);
        this.inventory = new InventoryStep(inventoryFailureRate);
        this.shipping = new ShippingStep(shippingFailureRate);
        this.orchestrator = new SagaOrchestrator(sagaStore, payment, inventory, shipping);
        this.relay = new OutboxRelay(bootstrap, outboxStore, 100L);
        this.consumer = new FulfillmentConsumer(bootstrap, orderTopic, eventTopic, groupId,
                FulfillmentConsumer.DEFAULT_WORKER_POOL_SIZE, consumerOverrides,
                ds, processedStore, outboxStore, dlq, orchestrator);

        this.relayWorker = new Thread(relay, "test-outbox-relay");
        this.consumerWorker = new Thread(consumer, "test-fulfillment-consumer");
    }

    void start() {
        relayWorker.start();
        consumerWorker.start();
    }

    void stopConsumerOnly() throws InterruptedException {
        consumer.stop();
        consumerWorker.join(10_000);
    }

    @Override
    public void close() {
        consumer.stop();
        relay.stop();
        try { consumerWorker.join(10_000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        try { relayWorker.join(10_000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        try { relay.close(); } catch (Exception ignored) {}
        try { dlq.close(); } catch (Exception ignored) {}
    }
}
