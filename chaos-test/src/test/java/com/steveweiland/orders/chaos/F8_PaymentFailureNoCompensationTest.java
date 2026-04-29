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

    private static Order sampleOrder() {
        return new Order(
                UUID.randomUUID().toString(),
                "cust-f8",
                List.of(new OrderItem("SKU-A", 1, new BigDecimal("9.99"))),
                new BigDecimal("9.99"),
                Instant.now());
    }
}
