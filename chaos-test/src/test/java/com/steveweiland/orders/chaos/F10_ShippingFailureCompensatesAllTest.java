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
 * F10 — Shipping step fails after payment + inventory success. (spec.md §6 F10)
 *
 * Compensations run in reverse order: inventory release, then payment refund.
 */
@Tag("chaos")
class F10_ShippingFailureCompensatesAllTest extends KafkaTestFixture {

    @Test
    void shippingFailureRollsBackBothPriorSteps() {
        SagaStore store = new SagaStore(dataSource());
        PaymentStep payment = new PaymentStep(0.0);
        InventoryStep inventory = new InventoryStep(0.0);
        ShippingStep shipping = new ShippingStep(1.0);       // always fails
        SagaOrchestrator orchestrator = new SagaOrchestrator(store, payment, inventory, shipping);

        Order order = sampleOrder();
        SagaResult result = orchestrator.run(order);

        assertEquals(SagaState.FAILED, result.finalState());
        assertEquals("shipping", result.failureStep());
        assertEquals(1, payment.executionCount());
        assertEquals(1, payment.compensationCount(),     "payment compensated last");
        assertEquals(1, inventory.executionCount());
        assertEquals(1, inventory.compensationCount(),   "inventory compensated first (reverse order)");
        assertEquals(1, shipping.executionCount(),       "shipping attempted");
        assertEquals(0, shipping.compensationCount(),    "shipping never succeeded; nothing to compensate");
        assertFalse(payment.charged(order.orderId()));
        assertFalse(inventory.reserved(order.orderId()));
        assertFalse(shipping.scheduled(order.orderId()));
    }

    private static Order sampleOrder() {
        return new Order(
                UUID.randomUUID().toString(),
                "cust-f10",
                List.of(new OrderItem("SKU-A", 1, new BigDecimal("9.99"))),
                new BigDecimal("9.99"),
                Instant.now());
    }
}
