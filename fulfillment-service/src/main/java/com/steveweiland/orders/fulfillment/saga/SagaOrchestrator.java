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
 * redelivery — on both the forward path (F11) and the compensation path
 * (F12): a saga row in any non-terminal state resumes exactly where it left
 * off rather than re-executing completed work or, worse, falling through to
 * a bogus COMPLETED.
 *
 * <p>The failure cause ({@code failure_step}/{@code failure_reason}) is
 * persisted by the transition that <em>enters</em> compensation, so a resumed
 * compensation can report the original failure without having seen the
 * original exception.
 *
 * <p>Compensations must be idempotent: a crash after a {@code compensate()}
 * call but before its state transition commits re-runs that compensation on
 * resume.
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

        return switch (saga.state()) {
            case PAYMENT_PENDING, INVENTORY_PENDING, SHIPPING_PENDING ->
                    executeForward(order, saga);
            case COMPENSATING_INVENTORY, COMPENSATING_PAYMENT ->
                    resumeCompensation(order, saga);
            // isTerminal() short-circuited above; reaching here is a logic bug,
            // never a reason to report success.
            case COMPLETED, FAILED ->
                    throw new IllegalStateException("terminal state leaked past short-circuit: " + saga.state());
        };
    }

    private SagaResult executeForward(Order order, SagaRecord saga) {
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
            }
            log.info("saga completed");
            return new SagaResult(saga.sagaId(), COMPLETED, null, null);
        } catch (StepFailedException e) {
            log.warn("saga step failed step={} reason={}", e.stepName(), e.getMessage());
            return failAndCompensate(order, saga.sagaId(), state, e);
        }
    }

    /**
     * Entry from a fresh step failure. {@code atFailure} is the state we were
     * ABOUT to execute when the step threw — i.e. the failed step's pending
     * state. The transition into compensation persists the failure cause
     * (see {@link SagaStore#beginCompensation}); from there the compensation
     * chain is shared with the resume path.
     */
    private SagaResult failAndCompensate(Order order, String sagaId, SagaState atFailure, StepFailedException cause) {
        switch (atFailure) {
            case PAYMENT_PENDING ->
                    // No prior steps to compensate.
                    store.recordFailure(sagaId, PAYMENT_PENDING, cause.stepName(), cause.getMessage());
            case INVENTORY_PENDING -> {
                store.beginCompensation(sagaId, INVENTORY_PENDING, COMPENSATING_PAYMENT,
                        cause.stepName(), cause.getMessage());
                compensateFrom(order, sagaId, COMPENSATING_PAYMENT);
            }
            case SHIPPING_PENDING -> {
                store.beginCompensation(sagaId, SHIPPING_PENDING, COMPENSATING_INVENTORY,
                        cause.stepName(), cause.getMessage());
                compensateFrom(order, sagaId, COMPENSATING_INVENTORY);
            }
            default -> throw new IllegalStateException("unexpected compensation source state " + atFailure);
        }
        return new SagaResult(sagaId, FAILED, cause.stepName(), cause.getMessage());
    }

    /** Redelivery of a saga that crashed mid-compensation (F12). */
    private SagaResult resumeCompensation(Order order, SagaRecord saga) {
        log.info("resuming compensation state={} failureStep={}", saga.state(), saga.failureStep());
        compensateFrom(order, saga.sagaId(), saga.state());
        return new SagaResult(saga.sagaId(), FAILED, saga.failureStep(), saga.failureReason());
    }

    /**
     * Walk the remaining compensations in reverse step order, committing each
     * transition before the next compensation so this chain is itself
     * resumable from any point.
     */
    private void compensateFrom(Order order, String sagaId, SagaState state) {
        if (state == COMPENSATING_INVENTORY) {
            inventory.compensate(order);
            store.transition(sagaId, COMPENSATING_INVENTORY, COMPENSATING_PAYMENT, null);
            state = COMPENSATING_PAYMENT;
        }
        if (state == COMPENSATING_PAYMENT) {
            payment.compensate(order);
            store.transition(sagaId, COMPENSATING_PAYMENT, FAILED, null);
        }
    }
}
