package com.steveweiland.orders.chaos;

import com.steveweiland.orders.common.Order;
import com.steveweiland.orders.common.OrderItem;
import com.steveweiland.orders.fulfillment.saga.InventoryStep;
import com.steveweiland.orders.fulfillment.saga.PaymentStep;
import com.steveweiland.orders.fulfillment.saga.SagaOrchestrator;
import com.steveweiland.orders.fulfillment.saga.SagaResult;
import com.steveweiland.orders.fulfillment.saga.SagaState;
import com.steveweiland.orders.fulfillment.saga.SagaStep;
import com.steveweiland.orders.fulfillment.saga.SagaStore;
import com.steveweiland.orders.fulfillment.saga.ShippingStep;
import com.steveweiland.orders.fulfillment.saga.StepFailedException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * F11 — Consumer crashes mid-saga. (spec.md §6 F11)
 *
 * Simulates a JVM crash during the inventory step by wrapping the real step
 * with one that throws {@link RuntimeException} on first call. The
 * orchestrator catches only {@link StepFailedException}, so a runtime
 * exception propagates out — leaving the saga in {@code INVENTORY_PENDING}
 * because the {@code PAYMENT_PENDING → INVENTORY_PENDING} transition was
 * already committed. A second {@link SagaOrchestrator#run} call (simulating
 * Kafka redelivery after restart) reads the existing saga row, picks up at
 * the inventory step, and runs the saga to completion.
 *
 * The contract under test: a step whose completion transition was COMMITTED
 * is never re-executed — the resume-from-state machinery skips it. (A crash
 * in the window between a step's execute() returning and its transition
 * committing re-runs that step on redelivery — inherent at-least-once until
 * per-step idempotency keys land in v3.2.0. This test crashes BEFORE the
 * inventory step's effect, so every count here is exactly 1.)
 */
@Tag("chaos")
class F11_SagaResumeAfterCrashTest extends KafkaTestFixture {

    @Test
    void resumesFromInventoryAfterSimulatedCrash() {
        SagaStore store = new SagaStore(dataSource());
        PaymentStep payment = new PaymentStep(0.0);
        InventoryStep inventory = new InventoryStep(0.0);
        ShippingStep shipping = new ShippingStep(0.0);

        // Wrap inventory so the first call throws an unchecked exception
        // (simulating "JVM crash during inventory.execute"). Second call
        // delegates to the real step.
        SagaStep crashingInventory = new CrashOnFirstCallStep(inventory);

        SagaOrchestrator firstRun = new SagaOrchestrator(store, payment, crashingInventory, shipping);
        Order order = sampleOrder();

        // First invocation crashes mid-saga.
        assertThrows(RuntimeException.class, () -> firstRun.run(order),
                "wrapped inventory throws on first call → propagates out of orchestrator");
        assertEquals(1, payment.executionCount(),     "payment ran in the first invocation");
        assertEquals(0, inventory.executionCount(),   "real inventory never reached on first invocation");

        // Second orchestrator instance over the same store + same in-memory
        // step instances. This models the JVM-restart case — fresh JVM, same
        // saga state in Postgres.
        SagaOrchestrator secondRun = new SagaOrchestrator(store, payment, crashingInventory, shipping);
        SagaResult result = secondRun.run(order);

        assertEquals(SagaState.COMPLETED, result.finalState());
        assertEquals(1, payment.executionCount(),     "payment NOT re-executed on resume");
        assertEquals(1, inventory.executionCount(),   "inventory ran exactly once (on resume)");
        assertEquals(1, shipping.executionCount(),    "shipping ran on the resumed saga");
        assertEquals(0, payment.compensationCount(),  "no compensation — saga succeeded");
        assertEquals(0, inventory.compensationCount());
        assertEquals(0, shipping.compensationCount());
    }

    private static Order sampleOrder() {
        return new Order(
                UUID.randomUUID().toString(),
                "cust-f11",
                List.of(new OrderItem("SKU-A", 1, new BigDecimal("9.99"))),
                new BigDecimal("9.99"),
                Instant.now());
    }

    private static final class CrashOnFirstCallStep implements SagaStep {
        private final SagaStep delegate;
        private final AtomicInteger calls = new AtomicInteger();

        CrashOnFirstCallStep(SagaStep delegate) {
            this.delegate = delegate;
        }

        @Override public String name() { return delegate.name(); }

        @Override
        public void execute(Order order) throws StepFailedException {
            if (calls.getAndIncrement() == 0) {
                throw new RuntimeException("simulated JVM crash during " + delegate.name());
            }
            delegate.execute(order);
        }

        @Override public void compensate(Order order) { delegate.compensate(order); }
    }
}
