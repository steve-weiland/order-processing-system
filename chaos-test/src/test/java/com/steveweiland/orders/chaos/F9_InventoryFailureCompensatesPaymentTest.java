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
 * F9 — Inventory step fails after payment success. (spec.md §6 F9)
 *
 * Payment is compensated (refunded) in reverse order. Saga transitions to
 * FAILED with {@code failure_step=inventory}.
 */
@Tag("chaos")
class F9_InventoryFailureCompensatesPaymentTest extends KafkaTestFixture {

    @Test
    void inventoryFailureRefundsPayment() {
        SagaStore store = new SagaStore(dataSource());
        PaymentStep payment = new PaymentStep(0.0);
        InventoryStep inventory = new InventoryStep(1.0);    // always fails
        ShippingStep shipping = new ShippingStep(0.0);
        SagaOrchestrator orchestrator = new SagaOrchestrator(store, payment, inventory, shipping);

        Order order = sampleOrder();
        SagaResult result = orchestrator.run(order);

        assertEquals(SagaState.FAILED, result.finalState());
        assertEquals("inventory", result.failureStep());
        assertEquals(1, payment.executionCount(),       "payment ran");
        assertEquals(1, payment.compensationCount(),    "payment compensated");
        assertEquals(1, inventory.executionCount(),     "inventory attempted");
        assertEquals(0, inventory.compensationCount(),  "inventory never succeeded; nothing to compensate");
        assertEquals(0, shipping.executionCount(),      "shipping never reached");
        assertFalse(payment.charged(order.orderId()),   "payment was charged then refunded — net: not charged");
    }

    private static Order sampleOrder() {
        return new Order(
                UUID.randomUUID().toString(),
                "cust-f9",
                List.of(new OrderItem("SKU-A", 1, new BigDecimal("9.99"))),
                new BigDecimal("9.99"),
                Instant.now());
    }
}
