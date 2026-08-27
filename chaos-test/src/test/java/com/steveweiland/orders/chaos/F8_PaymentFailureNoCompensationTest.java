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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * F8 — Payment step fails. (spec.md §6 F8)
 *
 * No prior steps to compensate. Saga transitions directly to FAILED.
 * The orchestrator records {@code failure_step=payment} on the saga row.
 */
@Tag("chaos")
class F8_PaymentFailureNoCompensationTest extends KafkaTestFixture {

    @Test
    void paymentFailureGoesStraightToFailed() {
        SagaStore store = new SagaStore(dataSource());
        PaymentStep payment = new PaymentStep(1.0);          // always fails
        InventoryStep inventory = new InventoryStep(0.0);
        ShippingStep shipping = new ShippingStep(0.0);
        SagaOrchestrator orchestrator = new SagaOrchestrator(store, payment, inventory, shipping);

        Order order = sampleOrder();
        SagaResult result = orchestrator.run(order);

        assertEquals(SagaState.FAILED, result.finalState());
        assertEquals("payment", result.failureStep());
        assertEquals(1, payment.executionCount(),  "payment.execute called exactly once");
        assertEquals(0, inventory.executionCount(), "inventory never executed");
        assertEquals(0, shipping.executionCount(),  "shipping never executed");
        assertEquals(0, payment.compensationCount(), "no compensation needed for first-step failure");
        assertFalse(payment.charged(order.orderId()),   "payment never succeeded");
    }

    /**
     * Full-stack shape of a failed saga (v3.1.0): the consumer still inserts a
     * processed_orders row (idempotency), but as status=FAILED with NO
     * fulfilled_at, no outbox row, and no OrderFulfilled event. Pre-V7 the row
     * said fulfilled_at=now() — the README's "fulfilled orders" query returned
     * failed orders.
     */
    @Test
    void failedSagaIsRecordedAsFailedNotFulfilled() throws Exception {
        com.steveweiland.orders.api.OrderProducer producer =
                new com.steveweiland.orders.api.OrderProducer(bootstrap(), orderTopic);
        try (V2Stack stack = new V2Stack(bootstrap(), orderTopic, eventTopic, dlqTopic,
                groupId, java.util.Map.of(), dataSource(), 1.0, 0.0, 0.0)) {   // payment always fails
            stack.start();

            Order order = sampleOrder();
            producer.send(order);

            org.awaitility.Awaitility.await()
                    .atMost(java.time.Duration.ofSeconds(15))
                    .until(() -> processedRow(order.orderId()) != null);

            ProcessedRow row = processedRow(order.orderId());
            assertEquals("FAILED", row.status(), "failed saga recorded as FAILED, not FULFILLED");
            assertEquals(null, row.fulfilledAt(), "a failed order has no fulfillment time");
            assertEquals(0, countRows("SELECT count(*) FROM outbox"), "no outbox row for a failed saga");
        } finally {
            producer.close();
        }
    }

    private ProcessedRow processedRow(String orderId) throws Exception {
        try (var c = dataSource().getConnection();
             var ps = c.prepareStatement(
                     "SELECT status, fulfilled_at FROM processed_orders WHERE order_id = ?::uuid")) {
            ps.setString(1, orderId);
            try (var rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return new ProcessedRow(rs.getString(1), rs.getTimestamp(2));
            }
        }
    }

    private int countRows(String sql) throws Exception {
        try (var c = dataSource().getConnection();
             var ps = c.prepareStatement(sql);
             var rs = ps.executeQuery()) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private record ProcessedRow(String status, java.sql.Timestamp fulfilledAt) {}

    private static Order sampleOrder() {
        return new Order(
                UUID.randomUUID().toString(),
                "cust-f8",
                List.of(new OrderItem("SKU-A", 1, new BigDecimal("9.99"))),
                new BigDecimal("9.99"),
                Instant.now());
    }
}
