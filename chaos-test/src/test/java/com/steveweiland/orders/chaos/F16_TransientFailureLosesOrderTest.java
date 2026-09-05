package com.steveweiland.orders.chaos;

import com.steveweiland.orders.api.OrderProducer;
import com.steveweiland.orders.common.Order;
import com.steveweiland.orders.common.OrderItem;
import com.steveweiland.orders.common.dlq.DlqProducer;
import com.steveweiland.orders.fulfillment.FulfillmentConsumer;
import com.steveweiland.orders.fulfillment.OutboxStore;
import com.steveweiland.orders.fulfillment.ProcessedOrdersStore;
import com.steveweiland.orders.fulfillment.saga.PaymentStep;
import com.steveweiland.orders.fulfillment.saga.SagaOrchestrator;
import com.steveweiland.orders.fulfillment.saga.SagaStep;
import com.steveweiland.orders.fulfillment.saga.SagaStore;
import com.steveweiland.orders.fulfillment.saga.InventoryStep;
import com.steveweiland.orders.fulfillment.saga.ShippingStep;
import com.steveweiland.orders.fulfillment.saga.StepFailedException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F16 — A transient worker failure permanently loses the order. (spec.md §6 F16)
 *
 * The pre-fix hole: when a worker failed (DB blip, raw RuntimeException out of
 * a saga step — anything that is NOT a handled StepFailedException), the batch
 * skipped its commit and the loop logged "will redeliver after rebalance" —
 * but kept polling FORWARD. The next successful batch on that partition then
 * committed offsets PAST the failed record. The order was never processed and
 * never redelivered: silently lost. F3 is blind to this (it kills the consumer
 * before commit); F11/F12 crash the JVM. Nothing exercised "survive a
 * transient failure and keep consuming".
 *
 * The fix under test: a failed batch seeks every partition of that batch back
 * to its first offset (paced, so an outage doesn't hot-spin) and lets the
 * re-poll redeliver. Batch dedupe, the pre-saga idempotency gate, and the
 * per-order advisory lock absorb the reprocessing of the batch's survivors.
 *
 * Mechanism: a single-partition topic (so the follow-up order is strictly
 * behind the flaky one), a payment step that throws a raw RuntimeException on
 * its first attempt for the target order, and a second order sent after the
 * first attempt has failed. Both orders must reach processed_orders as
 * FULFILLED with no rebalance and no consumer crash.
 */
@Tag("chaos")
class F16_TransientFailureLosesOrderTest extends KafkaTestFixture {

    /** Payment that fails its FIRST attempt for one order with a raw (non-step) exception. */
    static final class FlakyOncePayment implements SagaStep {
        private final PaymentStep delegate = new PaymentStep(0.0);
        private final String flakyOrderId;
        final AtomicInteger flakyAttempts = new AtomicInteger();

        FlakyOncePayment(String flakyOrderId) {
            this.flakyOrderId = flakyOrderId;
        }

        @Override
        public String name() { return delegate.name(); }

        @Override
        public void execute(Order order) throws StepFailedException {
            if (order.orderId().equals(flakyOrderId) && flakyAttempts.incrementAndGet() == 1) {
                // Raw RuntimeException = infrastructure failure, not a business
                // decline — the orchestrator must NOT treat it as a saga failure.
                throw new RuntimeException("transient infra failure (injected)");
            }
            delegate.execute(order);
        }

        @Override
        public void compensate(Order order) { delegate.compensate(order); }
    }

    @Test
    void transientWorkerFailureMustNotLoseTheOrder() throws Exception {
        // Single partition: the follow-up order lands strictly BEHIND the flaky
        // one, so a forward-moving commit demonstrably skips past it (pre-fix).
        String singlePartitionTopic = orderTopic + "-1p";
        try (AdminClient admin = adminClient()) {
            admin.createTopics(List.of(new NewTopic(singlePartitionTopic, 1, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS);
        }

        Order flaky = sampleOrder("cust-f16-flaky");
        Order follower = sampleOrder("cust-f16-follower");
        FlakyOncePayment payment = new FlakyOncePayment(flaky.orderId());
        SagaOrchestrator orchestrator = new SagaOrchestrator(new SagaStore(dataSource()),
                payment, new InventoryStep(0.0), new ShippingStep(0.0));

        OrderProducer producer = new OrderProducer(bootstrap(), singlePartitionTopic);
        DlqProducer dlq = new DlqProducer(bootstrap(), dlqTopic);
        FulfillmentConsumer consumer = new FulfillmentConsumer(bootstrap(), singlePartitionTopic,
                eventTopic, groupId, FulfillmentConsumer.DEFAULT_WORKER_POOL_SIZE, Map.of(),
                dataSource(), new ProcessedOrdersStore(), new OutboxStore(dataSource()), dlq, orchestrator);
        Thread worker = new Thread(consumer, "test-f16-consumer");
        try {
            worker.start();

            producer.send(flaky);
            // Let the first attempt fail before the follower arrives, so the
            // follower's batch is the one whose commit would skip the victim.
            Awaitility.await().atMost(Duration.ofSeconds(15))
                    .until(() -> payment.flakyAttempts.get() >= 1);
            producer.send(follower);

            // REGRESSION GUARD: pre-fix, the follower fulfills, its commit moves
            // past the flaky order's offset, and the flaky order never lands —
            // this await times out.
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertEquals("FULFILLED", statusOf(follower.orderId()), "follower order fulfilled");
                assertEquals("FULFILLED", statusOf(flaky.orderId()),
                        "transiently-failed order recovered via seek-back redelivery, same group generation");
            });
            assertFalse(consumer.crashed(), "transient failure must not kill the consumer");
            assertTrue(payment.flakyAttempts.get() >= 2, "the flaky order was retried");
        } finally {
            consumer.stop();
            worker.join(10_000);
            producer.close();
            try (AdminClient admin = adminClient()) {
                admin.deleteTopics(List.of(singlePartitionTopic));
            } catch (Exception ignored) {}
        }
    }

    private String statusOf(String orderId) throws Exception {
        try (var c = dataSource().getConnection();
             var ps = c.prepareStatement("SELECT status FROM processed_orders WHERE order_id = ?::uuid")) {
            ps.setString(1, orderId);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    private static Order sampleOrder(String customerId) {
        return new Order(
                UUID.randomUUID().toString(),
                customerId,
                List.of(new OrderItem("SKU-A", 1, new BigDecimal("9.99"))),
                new BigDecimal("9.99"),
                Instant.now());
    }
}
