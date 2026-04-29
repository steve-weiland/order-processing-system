package com.steveweiland.orders.fulfillment.saga;

public enum SagaState {
    PAYMENT_PENDING,
    INVENTORY_PENDING,
    SHIPPING_PENDING,
    COMPLETED,
    COMPENSATING_INVENTORY,
    COMPENSATING_PAYMENT,
    FAILED;

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED;
    }
}
