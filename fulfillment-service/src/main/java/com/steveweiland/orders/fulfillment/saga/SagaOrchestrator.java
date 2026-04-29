package com.steveweiland.orders.fulfillment.saga;

import com.steveweiland.orders.common.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.steveweiland.orders.fulfillment.saga.SagaState.COMPENSATING_INVENTORY;
import static com.steveweiland.orders.fulfillment.saga.SagaState.COMPENSATING_PAYMENT;
import static com.steveweiland.orders.fulfillment.saga.SagaState.COMPLETED;
import static com.steveweiland.orders.fulfillment.saga.SagaState.FAILED;
import static com.steveweiland.orders.fulfillment.saga.SagaState.INVENTORY_PENDING;
import static com.steveweiland.orders.fulfillment.saga.SagaState.PAYMENT_PENDING;
import static com.steveweiland.orders.fulfillment.saga.SagaState.SHIPPING_PENDING;

/**
 * Synchronous, in-process orchestrator. Drives the saga through three steps
 * (payment → inventory → shipping). Persists every state transition before
 * and after each step's invocation so the orchestrator is resumable on
 * redelivery: if a saga row exists in a non-terminal state, the orchestrator
 * picks up at the next pending step rather than re-executing completed ones.
 */
public final class SagaOrchestrator {
    private static final Logger log = LoggerFactory.getLogger(SagaOrchestrator.class);

    private final SagaStore store;
    private final SagaStep payment;
    private final SagaStep inventory;
    private final SagaStep shipping;

    public SagaOrchestrator(SagaStore store, SagaStep payment, SagaStep inventory, SagaStep shipping) {
        this.store = store;
        this.payment = payment;
        this.inventory = inventory;
        this.shipping = shipping;
    }

    public SagaResult run(Order order) {
        SagaRecord saga = store.startOrResume(order.orderId());
        if (saga.isTerminal()) {
            log.info("saga already terminal state={}", saga.state());
            return new SagaResult(saga.sagaId(), saga.state(), saga.failureStep(), saga.failureReason());
        }

        SagaState state = saga.state();
        try {
            if (state == PAYMENT_PENDING) {
                payment.execute(order);
                store.transition(saga.sagaId(), PAYMENT_PENDING, INVENTORY_PENDING, "payment_done_at");
                state = INVENTORY_PENDING;
            }
            if (state == INVENTORY_PENDING) {
                inventory.execute(order);
                store.transition(saga.sagaId(), INVENTORY_PENDING, SHIPPING_PENDING, "inventory_done_at");
                state = SHIPPING_PENDING;
            }
            if (state == SHIPPING_PENDING) {
                shipping.execute(order);
                store.transition(saga.sagaId(), SHIPPING_PENDING, COMPLETED, "shipping_done_at");
                state = COMPLETED;
            }
            log.info("saga completed");
            return new SagaResult(saga.sagaId(), COMPLETED, null, null);
        } catch (StepFailedException e) {
            log.warn("saga step failed step={} reason={}", e.stepName(), e.getMessage());
            return compensate(order, saga.sagaId(), state, e);
        }
    }

    private SagaResult compensate(Order order, String sagaId, SagaState atFailure, StepFailedException cause) {
        // Walk completed steps in reverse. The state passed in is the state we
        // were ABOUT to execute when the step threw — i.e. the failed step's
        // pending state.
        switch (atFailure) {
            case PAYMENT_PENDING -> {
                // No prior steps to compensate.
                store.recordFailure(sagaId, PAYMENT_PENDING, cause.stepName(), cause.getMessage());
            }
            case INVENTORY_PENDING -> {
                store.transition(sagaId, INVENTORY_PENDING, COMPENSATING_PAYMENT, null);
                payment.compensate(order);
                store.recordFailure(sagaId, COMPENSATING_PAYMENT, cause.stepName(), cause.getMessage());
            }
            case SHIPPING_PENDING -> {
                store.transition(sagaId, SHIPPING_PENDING, COMPENSATING_INVENTORY, null);
                inventory.compensate(order);
                store.transition(sagaId, COMPENSATING_INVENTORY, COMPENSATING_PAYMENT, null);
                payment.compensate(order);
                store.recordFailure(sagaId, COMPENSATING_PAYMENT, cause.stepName(), cause.getMessage());
            }
            default -> throw new IllegalStateException("unexpected compensation source state " + atFailure);
        }
        return new SagaResult(sagaId, FAILED, cause.stepName(), cause.getMessage());
    }
}
