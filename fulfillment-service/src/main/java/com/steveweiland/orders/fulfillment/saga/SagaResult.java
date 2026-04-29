package com.steveweiland.orders.fulfillment.saga;

public record SagaResult(String sagaId, SagaState finalState, String failureStep, String failureReason) {

    public boolean isCompleted() {
        return finalState == SagaState.COMPLETED;
    }

    public boolean isFailed() {
        return finalState == SagaState.FAILED;
    }
}
