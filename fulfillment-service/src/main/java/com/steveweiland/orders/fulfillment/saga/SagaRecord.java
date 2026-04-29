package com.steveweiland.orders.fulfillment.saga;

import java.time.Instant;

public record SagaRecord(
        String sagaId,
        String orderId,
        SagaState state,
        Instant paymentDoneAt,
        Instant inventoryDoneAt,
        Instant shippingDoneAt,
        String failureStep,
        String failureReason) {

    public boolean isTerminal() {
        return state.isTerminal();
    }
}
