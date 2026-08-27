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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F12 — Crash mid-compensation. (spec.md §6 F12)
 *
 * The V3.0.0 bug: {@code SagaOrchestrator.run} handled only the three
 * {@code *_PENDING} states. A saga redelivered in {@code COMPENSATING_*}
 * (non-terminal, so it passed the isTerminal() gate) matched none of the
 * forward blocks and fell through to a bogus {@code COMPLETED} — emitting an
 * OrderFulfilled event for a failed, half-compensated (still-charged) order,
 * with the sagas row stranded in {@code COMPENSATING_*} forever.
 *
 * The contract under test: compensation is resumable from ANY point in the
 * chain. The failure cause is persisted by the transition that enters
 * compensation, so the resumed run reports the original failure_step even
 * though the original exception is gone with the crashed JVM.
 */
@Tag("chaos")
class F12_CompensationResumeAfterCrashTest extends KafkaTestFixture {

    /** Crash during inventory.compensate → resume from COMPENSATING_INVENTORY. */
    @Test
    void resumesCompensationFromCompensatingInventory() throws Exception {
        SagaStore store = new SagaStore(dataSource());
        PaymentStep payment = new PaymentStep(0.0);
        InventoryStep inventory = new InventoryStep(0.0);
        ShippingStep shipping = new ShippingStep(1.0);       // always fails → full compensation chain

        SagaStep crashingInventory = new CrashOnFirstCompensateStep(inventory);
        Order order = sampleOrder("cust-f12-a");

        // First invocation: payment + inventory succeed, shipping fails,
        // compensation begins, inventory.compensate "crashes the JVM".
        SagaOrchestrator firstRun = new SagaOrchestrator(store, payment, crashingInventory, shipping);
        assertThrows(RuntimeException.class, () -> firstRun.run(order),
                "simulated crash during inventory.compensate propagates");
        assertEquals(SagaState.COMPENSATING_INVENTORY.name(), sagaColumn(order, "state"),
                "crash left the saga mid-compensation");
        assertEquals("shipping", sagaColumn(order, "failure_step"),
                "failure cause was persisted when compensation BEGAN, not at the end");
        assertEquals(0, inventory.compensationCount(), "real inventory.compensate never ran before the crash");
        assertEquals(0, payment.compensationCount(), "payment.compensate never reached before the crash");

        // Second invocation (fresh orchestrator = restarted JVM, same Postgres):
        // must finish the compensation chain, NOT report COMPLETED.
        SagaOrchestrator secondRun = new SagaOrchestrator(store, payment, crashingInventory, shipping);
        SagaResult result = secondRun.run(order);

        assertFalse(result.isCompleted(),
                "REGRESSION GUARD: pre-fix code fell through to COMPLETED here");
        assertEquals(SagaState.FAILED, result.finalState());
        assertEquals("shipping", result.failureStep(), "resumed result carries the original failure step");
        assertEquals(SagaState.FAILED.name(), sagaColumn(order, "state"), "saga row reached terminal FAILED");
        assertEquals(1, inventory.compensationCount(), "inventory released on resume");
        assertEquals(1, payment.compensationCount(), "payment refunded on resume, in reverse order");
        assertFalse(payment.charged(order.orderId()), "net effect: customer not charged");
    }

    /** Crash during payment.compensate → resume from COMPENSATING_PAYMENT. */
    @Test
    void resumesCompensationFromCompensatingPayment() throws Exception {
        SagaStore store = new SagaStore(dataSource());
        PaymentStep payment = new PaymentStep(0.0);
        InventoryStep inventory = new InventoryStep(0.0);
        ShippingStep shipping = new ShippingStep(1.0);

        SagaStep crashingPayment = new CrashOnFirstCompensateStep(payment);
        Order order = sampleOrder("cust-f12-b");

        // First invocation: inventory compensates cleanly, then the crash hits
        // during payment.compensate — one transition further into the chain.
        SagaOrchestrator firstRun = new SagaOrchestrator(store, crashingPayment, inventory, shipping);
        assertThrows(RuntimeException.class, () -> firstRun.run(order));
        assertEquals(SagaState.COMPENSATING_PAYMENT.name(), sagaColumn(order, "state"));
        assertEquals(1, inventory.compensationCount(), "inventory compensated before the crash");
        assertEquals(0, payment.compensationCount());

        SagaOrchestrator secondRun = new SagaOrchestrator(store, crashingPayment, inventory, shipping);
        SagaResult result = secondRun.run(order);

        assertEquals(SagaState.FAILED, result.finalState());
        assertEquals("shipping", result.failureStep());
        assertEquals(1, inventory.compensationCount(),
                "already-completed compensation NOT re-executed on resume");
        assertEquals(1, payment.compensationCount(), "payment refunded on resume");
        assertEquals(SagaState.FAILED.name(), sagaColumn(order, "state"));
    }

    /** Terminal FAILED short-circuits on a third delivery — no compensation re-runs. */
    @Test
    void terminalFailedShortCircuitsAfterResume() throws Exception {
        SagaStore store = new SagaStore(dataSource());
        PaymentStep payment = new PaymentStep(0.0);
        InventoryStep inventory = new InventoryStep(1.0);    // fails → compensate payment only
        ShippingStep shipping = new ShippingStep(0.0);
        SagaOrchestrator orchestrator = new SagaOrchestrator(store, payment, inventory, shipping);
        Order order = sampleOrder("cust-f12-c");

        SagaResult first = orchestrator.run(order);
        assertEquals(SagaState.FAILED, first.finalState());
        assertEquals(1, payment.compensationCount());

        SagaResult redelivery = orchestrator.run(order);
        assertEquals(SagaState.FAILED, redelivery.finalState());
        assertEquals("inventory", redelivery.failureStep(), "terminal result still carries the failure cause");
        assertEquals(1, payment.compensationCount(), "no compensation re-run on terminal re-entry");
        assertTrue(redelivery.isFailed());
    }

    private String sagaColumn(Order order, String column) throws Exception {
        try (Connection c = dataSource().getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT " + column + " FROM sagas WHERE order_id = ?::uuid")) {
            ps.setString(1, order.orderId());
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "saga row exists for " + order.orderId());
                return rs.getString(1);
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

    /** Delegates, except compensate() throws RuntimeException on its first call. */
    private static final class CrashOnFirstCompensateStep implements SagaStep {
        private final SagaStep delegate;
        private final AtomicInteger compensateCalls = new AtomicInteger();

        CrashOnFirstCompensateStep(SagaStep delegate) {
            this.delegate = delegate;
        }

        @Override public String name() { return delegate.name(); }

        @Override
        public void execute(Order order) throws StepFailedException {
            delegate.execute(order);
        }

        @Override
        public void compensate(Order order) {
            if (compensateCalls.getAndIncrement() == 0) {
                throw new RuntimeException("simulated JVM crash during " + delegate.name() + ".compensate");
            }
            delegate.compensate(order);
        }
    }
}
