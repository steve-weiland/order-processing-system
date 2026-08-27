package com.steveweiland.orders.fulfillment.saga;

import com.steveweiland.orders.common.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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
 * <p>Concurrent invocations for the same order (duplicate records in one
 * poll batch, or two consumer instances during rebalance overlap) are
 * serialized by a per-order Postgres <em>session</em> advisory lock (F13).
 * The loser blocks until the winner finishes, then re-reads the saga row
 * under the lock and short-circuits on the terminal state — each step
 * executes exactly once per saga, never once per delivery. The entire run
 * uses a single pooled connection: the lock and every state read/write ride
 * the same session (a second connection per worker would exhaust the pool),
 * and a session lock — unlike {@code pg_advisory_xact_lock} — survives the
 * per-transition autocommits that crash-resume requires. If the JVM dies
 * mid-saga, the session dies with it and Postgres releases the lock.
 *
 * <p>Under the lock a lost compare-and-swap is a logic bug, not a race — so
 * every CAS result is enforced and a loss fails loudly rather than letting
 * the invocation keep executing steps it no longer owns.
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
    private final DataSource ds;
    private final SagaStep payment;
    private final SagaStep inventory;
    private final SagaStep shipping;

    public SagaOrchestrator(SagaStore store, SagaStep payment, SagaStep inventory, SagaStep shipping) {
        this.store = store;
        this.ds = store.dataSource();
        this.payment = payment;
        this.inventory = inventory;
        this.shipping = shipping;
    }

    public SagaResult run(Order order) {
        try (Connection c = ds.getConnection()) {
            advisoryLock(c, order.orderId(), true);
            try {
                return runLocked(c, order);
            } finally {
                advisoryLock(c, order.orderId(), false);
            }
        } catch (SQLException e) {
            throw new RuntimeException("saga connection failed orderId=" + order.orderId(), e);
        }
    }

    private SagaResult runLocked(Connection c, Order order) {
        // Load AFTER acquiring the lock: a concurrent invocation may have
        // driven the saga to terminal while we were blocked.
        SagaRecord saga = store.startOrResume(c, order.orderId());
        if (saga.isTerminal()) {
            log.info("saga already terminal state={}", saga.state());
            return new SagaResult(saga.sagaId(), saga.state(), saga.failureStep(), saga.failureReason());
        }

        return switch (saga.state()) {
            case PAYMENT_PENDING, INVENTORY_PENDING, SHIPPING_PENDING ->
                    executeForward(c, order, saga);
            case COMPENSATING_INVENTORY, COMPENSATING_PAYMENT ->
                    resumeCompensation(c, order, saga);
            // isTerminal() short-circuited above; reaching here is a logic bug,
            // never a reason to report success.
            case COMPLETED, FAILED ->
                    throw new IllegalStateException("terminal state leaked past short-circuit: " + saga.state());
        };
    }

    private SagaResult executeForward(Connection c, Order order, SagaRecord saga) {
        SagaState state = saga.state();
        try {
            if (state == PAYMENT_PENDING) {
                payment.execute(order);
                requireCas(store.transition(c, saga.sagaId(), PAYMENT_PENDING, INVENTORY_PENDING, "payment_done_at"),
                        saga.sagaId(), PAYMENT_PENDING, INVENTORY_PENDING);
                state = INVENTORY_PENDING;
            }
            if (state == INVENTORY_PENDING) {
                inventory.execute(order);
                requireCas(store.transition(c, saga.sagaId(), INVENTORY_PENDING, SHIPPING_PENDING, "inventory_done_at"),
                        saga.sagaId(), INVENTORY_PENDING, SHIPPING_PENDING);
                state = SHIPPING_PENDING;
            }
            if (state == SHIPPING_PENDING) {
                shipping.execute(order);
                requireCas(store.transition(c, saga.sagaId(), SHIPPING_PENDING, COMPLETED, "shipping_done_at"),
                        saga.sagaId(), SHIPPING_PENDING, COMPLETED);
            }
            log.info("saga completed");
            return new SagaResult(saga.sagaId(), COMPLETED, null, null);
        } catch (StepFailedException e) {
            log.warn("saga step failed step={} reason={}", e.stepName(), e.getMessage());
            return failAndCompensate(c, order, saga.sagaId(), state, e);
        }
    }

    /**
     * Entry from a fresh step failure. {@code atFailure} is the state we were
     * ABOUT to execute when the step threw — i.e. the failed step's pending
     * state. The transition into compensation persists the failure cause
     * (see {@link SagaStore#beginCompensation}); from there the compensation
     * chain is shared with the resume path.
     */
    private SagaResult failAndCompensate(Connection c, Order order, String sagaId,
                                         SagaState atFailure, StepFailedException cause) {
        switch (atFailure) {
            case PAYMENT_PENDING ->
                    // No prior steps to compensate.
                    requireCas(store.recordFailure(c, sagaId, PAYMENT_PENDING, cause.stepName(), cause.getMessage()),
                            sagaId, PAYMENT_PENDING, FAILED);
            case INVENTORY_PENDING -> {
                requireCas(store.beginCompensation(c, sagaId, INVENTORY_PENDING, COMPENSATING_PAYMENT,
                                cause.stepName(), cause.getMessage()),
                        sagaId, INVENTORY_PENDING, COMPENSATING_PAYMENT);
                compensateFrom(c, order, sagaId, COMPENSATING_PAYMENT);
            }
            case SHIPPING_PENDING -> {
                requireCas(store.beginCompensation(c, sagaId, SHIPPING_PENDING, COMPENSATING_INVENTORY,
                                cause.stepName(), cause.getMessage()),
                        sagaId, SHIPPING_PENDING, COMPENSATING_INVENTORY);
                compensateFrom(c, order, sagaId, COMPENSATING_INVENTORY);
            }
            default -> throw new IllegalStateException("unexpected compensation source state " + atFailure);
        }
        return new SagaResult(sagaId, FAILED, cause.stepName(), cause.getMessage());
    }

    /** Redelivery of a saga that crashed mid-compensation (F12). */
    private SagaResult resumeCompensation(Connection c, Order order, SagaRecord saga) {
        log.info("resuming compensation state={} failureStep={}", saga.state(), saga.failureStep());
        compensateFrom(c, order, saga.sagaId(), saga.state());
        return new SagaResult(saga.sagaId(), FAILED, saga.failureStep(), saga.failureReason());
    }

    /**
     * Walk the remaining compensations in reverse step order, committing each
     * transition before the next compensation so this chain is itself
     * resumable from any point.
     */
    private void compensateFrom(Connection c, Order order, String sagaId, SagaState state) {
        if (state == COMPENSATING_INVENTORY) {
            inventory.compensate(order);
            requireCas(store.transition(c, sagaId, COMPENSATING_INVENTORY, COMPENSATING_PAYMENT, null),
                    sagaId, COMPENSATING_INVENTORY, COMPENSATING_PAYMENT);
            state = COMPENSATING_PAYMENT;
        }
        if (state == COMPENSATING_PAYMENT) {
            payment.compensate(order);
            requireCas(store.transition(c, sagaId, COMPENSATING_PAYMENT, FAILED, null),
                    sagaId, COMPENSATING_PAYMENT, FAILED);
        }
    }

    private static void requireCas(boolean won, String sagaId, SagaState from, SagaState to) {
        if (!won) {
            throw new IllegalStateException("lost saga CAS sagaId=" + sagaId + " " + from + "→" + to
                    + " — concurrent transition under the per-order lock should be impossible");
        }
    }

    private static void advisoryLock(Connection c, String orderId, boolean acquire) {
        String fn = acquire ? "pg_advisory_lock" : "pg_advisory_unlock";
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT " + fn + "(hashtextextended(?, 0))")) {
            ps.setString(1, orderId);
            ps.execute();
        } catch (SQLException e) {
            // On acquire: fail the invocation (redelivery retries). On release:
            // a broken connection's session is gone and Postgres drops its
            // locks; Hikari evicts broken connections on close.
            throw new RuntimeException("saga " + fn + " failed orderId=" + orderId, e);
        }
    }
}
