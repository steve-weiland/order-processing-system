package com.steveweiland.orders.chaos;

import com.steveweiland.orders.common.Order;
import com.steveweiland.orders.common.OrderItem;
import com.steveweiland.orders.fulfillment.saga.InventoryStep;
import com.steveweiland.orders.fulfillment.saga.PaymentStep;
import com.steveweiland.orders.fulfillment.saga.SagaOrchestrator;
import com.steveweiland.orders.fulfillment.saga.SagaResult;
import com.steveweiland.orders.fulfillment.saga.SagaState;
import com.steveweiland.orders.fulfillment.saga.SagaStore;
import com.steveweiland.orders.fulfillment.saga.ShippingStep;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F13 — Concurrent duplicate deliveries double-execute saga steps. (spec.md §6 F13)
 *
 * The V3.0.0 bug: two deliveries of the same order (duplicate records in one
 * poll batch dispatched onto the parallel executor, or two consumer instances
 * during rebalance overlap) both loaded the saga at PAYMENT_PENDING and both
 * ran payment.execute() — the customer was charged twice. The CAS transition
 * existed (OPS-213) but its result was discarded, so the loser kept executing
 * the remaining steps too. Invisible to F1, which asserts the EVENT count
 * (correct — processed_orders dedupes the outbox row), not the execution
 * count.
 *
 * The fix under test: a per-order Postgres session advisory lock serializes
 * concurrent runs. The loser blocks, then re-reads the saga row under the
 * lock and short-circuits on the winner's terminal state — both invocations
 * return the same result and each step executed exactly once.
 */
@Tag("chaos")
class F13_ConcurrentSagaDoubleExecutionTest extends KafkaTestFixture {

    @Test
    void concurrentRunsExecuteEachStepExactlyOnce() throws Exception {
        SagaStore store = new SagaStore(dataSource());
        PaymentStep payment = new PaymentStep(0.0);
        InventoryStep inventory = new InventoryStep(0.0);
        ShippingStep shipping = new ShippingStep(0.0);
        SagaOrchestrator orchestrator = new SagaOrchestrator(store, payment, inventory, shipping);
        Order order = sampleOrder("cust-f13-a");

        List<SagaResult> results = runConcurrently(2, () -> orchestrator.run(order));

        for (SagaResult r : results) {
            assertEquals(SagaState.COMPLETED, r.finalState(), "both deliveries observe the same terminal result");
        }
        assertEquals(1, payment.executionCount(),
                "REGRESSION GUARD: pre-fix code charged the customer twice here");
        assertEquals(1, inventory.executionCount(), "inventory reserved exactly once");
        assertEquals(1, shipping.executionCount(), "shipping executed exactly once");
        assertEquals(0, payment.compensationCount());
        assertTrue(payment.charged(order.orderId()), "completed saga leaves the charge in place");
    }

    @Test
    void concurrentRunsCompensateExactlyOnce() throws Exception {
        SagaStore store = new SagaStore(dataSource());
        PaymentStep payment = new PaymentStep(0.0);
        InventoryStep inventory = new InventoryStep(1.0);    // always fails → compensation path
        ShippingStep shipping = new ShippingStep(0.0);
        SagaOrchestrator orchestrator = new SagaOrchestrator(store, payment, inventory, shipping);
        Order order = sampleOrder("cust-f13-b");

        List<SagaResult> results = runConcurrently(2, () -> orchestrator.run(order));

        for (SagaResult r : results) {
            assertEquals(SagaState.FAILED, r.finalState());
            assertEquals("inventory", r.failureStep(), "loser's terminal result carries the failure cause");
        }
        assertEquals(1, payment.executionCount(), "charged once");
        assertEquals(1, payment.compensationCount(), "refunded once — not once per delivery");
        assertEquals(1, inventory.executionCount(), "failing step attempted once, not once per delivery");
    }

    private static List<SagaResult> runConcurrently(int n, Callable<SagaResult> task) throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        try (ExecutorService pool = Executors.newFixedThreadPool(n)) {
            List<Future<SagaResult>> futures = java.util.stream.IntStream.range(0, n)
                    .mapToObj(i -> pool.submit(() -> {
                        start.await();
                        return task.call();
                    }))
                    .toList();
            start.countDown();
            java.util.ArrayList<SagaResult> results = new java.util.ArrayList<>();
            for (Future<SagaResult> f : futures) {
                results.add(f.get(30, TimeUnit.SECONDS));
            }
            return results;
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
