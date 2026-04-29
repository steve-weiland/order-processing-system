package com.steveweiland.orders.fulfillment.saga;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;

/**
 * Persists saga state. State transitions are compare-and-swap UPDATEs so two
 * orchestrator invocations cannot both advance from the same state.
 */
public final class SagaStore {
    private final DataSource ds;

    public SagaStore(DataSource ds) {
        this.ds = ds;
    }

    /**
     * Atomically start a saga for the given orderId, or return the existing row
     * if one already exists. The atomic INSERT … ON CONFLICT DO NOTHING is the
     * gate against double-starting a saga under concurrent redelivery.
     */
    public SagaRecord startOrResume(String orderId) {
        try (Connection c = ds.getConnection()) {
            // Try to insert; on conflict, do nothing.
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO sagas (saga_id, order_id, state) " +
                            "VALUES (?::uuid, ?::uuid, 'PAYMENT_PENDING') " +
                            "ON CONFLICT (saga_id) DO NOTHING")) {
                ps.setString(1, orderId);
                ps.setString(2, orderId);
                ps.executeUpdate();
            }
            return loadByOrderId(c, orderId).orElseThrow(
                    () -> new IllegalStateException("saga row missing after upsert orderId=" + orderId));
        } catch (SQLException e) {
            throw new RuntimeException("saga startOrResume failed orderId=" + orderId, e);
        }
    }

    private Optional<SagaRecord> loadByOrderId(Connection c, String orderId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT saga_id::text, order_id::text, state, payment_done_at, inventory_done_at, " +
                        "shipping_done_at, failure_step, failure_reason " +
                        "FROM sagas WHERE order_id = ?::uuid")) {
            ps.setString(1, orderId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new SagaRecord(
                        rs.getString(1),
                        rs.getString(2),
                        SagaState.valueOf(rs.getString(3)),
                        toInstant(rs.getTimestamp(4)),
                        toInstant(rs.getTimestamp(5)),
                        toInstant(rs.getTimestamp(6)),
                        rs.getString(7),
                        rs.getString(8)));
            }
        }
    }

    /**
     * Compare-and-swap state transition. Returns true if exactly one row was
     * updated (the caller still owns the saga); false if a concurrent attempt
     * had already advanced the state.
     */
    public boolean transition(String sagaId, SagaState from, SagaState to, String stepDoneColumn) {
        String setColumn = stepDoneColumn == null ? "" : ", " + stepDoneColumn + " = now()";
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "UPDATE sagas SET state = ?, updated_at = now()" + setColumn +
                             " WHERE saga_id = ?::uuid AND state = ?")) {
            ps.setString(1, to.name());
            ps.setString(2, sagaId);
            ps.setString(3, from.name());
            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new RuntimeException("saga transition failed sagaId=" + sagaId
                    + " from=" + from + " to=" + to, e);
        }
    }

    public boolean recordFailure(String sagaId, SagaState from, String failureStep, String reason) {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "UPDATE sagas SET state = 'FAILED', failure_step = ?, failure_reason = ?, " +
                             "updated_at = now() WHERE saga_id = ?::uuid AND state = ?")) {
            ps.setString(1, failureStep);
            ps.setString(2, reason);
            ps.setString(3, sagaId);
            ps.setString(4, from.name());
            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new RuntimeException("saga recordFailure failed sagaId=" + sagaId, e);
        }
    }

    private static Instant toInstant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }
}
