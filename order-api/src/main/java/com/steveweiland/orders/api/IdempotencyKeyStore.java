package com.steveweiland.orders.api;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Postgres-backed store for the optional {@code Idempotency-Key} HTTP header,
 * with a two-phase (pending → confirmed) lifecycle.
 *
 * <p>{@link #claim} atomically inserts {@code (key, generatedOrderId)} with
 * {@code confirmed_at} NULL; {@link #confirm} stamps it after the Kafka
 * produce is acked. The three outcomes of a claim:
 * <ul>
 *   <li>{@code FRESH} — this caller owns the key; produce, then confirm.</li>
 *   <li>{@code REPLAY_CONFIRMED} — a previous request produced successfully;
 *       return the stored orderId, produce nothing.</li>
 *   <li>{@code REPLAY_PENDING} — a previous attempt claimed the key but its
 *       produce never succeeded. The caller MUST re-produce with the stored
 *       orderId (duplicates on the topic are absorbed downstream by the
 *       saga's terminal short-circuit + processed_orders) and then confirm.
 *       Without this state, a produce failure poisoned the key forever: the
 *       retry replayed an orderId that never reached Kafka.</li>
 * </ul>
 */
public final class IdempotencyKeyStore {
    private final DataSource ds;

    public IdempotencyKeyStore(DataSource ds) {
        this.ds = ds;
    }

    public Decision claim(String key, String generatedOrderId) {
        try (Connection c = ds.getConnection()) {
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO idempotency_keys (key, order_id) VALUES (?, ?::uuid) " +
                            "ON CONFLICT (key) DO NOTHING")) {
                ps.setString(1, key);
                ps.setString(2, generatedOrderId);
                int rows = ps.executeUpdate();
                if (rows == 1) {
                    return new Decision(generatedOrderId, State.FRESH);
                }
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT order_id::text, confirmed_at FROM idempotency_keys WHERE key = ?")) {
                ps.setString(1, key);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        boolean confirmed = rs.getTimestamp(2) != null;
                        return new Decision(rs.getString(1),
                                confirmed ? State.REPLAY_CONFIRMED : State.REPLAY_PENDING);
                    }
                }
            }
            throw new IllegalStateException("idempotency_keys lost a row we just inserted: key=" + key);
        } catch (SQLException e) {
            throw new RuntimeException("idempotency claim failed", e);
        }
    }

    /** Mark the key's produce as acked. Idempotent; keeps the first stamp. */
    public void confirm(String key) {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "UPDATE idempotency_keys SET confirmed_at = now() " +
                             "WHERE key = ? AND confirmed_at IS NULL")) {
            ps.setString(1, key);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("idempotency confirm failed for key=" + key, e);
        }
    }

    public enum State { FRESH, REPLAY_CONFIRMED, REPLAY_PENDING }

    public record Decision(String orderId, State state) {
        /** True when the caller must produce (fresh claim or unconfirmed replay). */
        public boolean mustProduce() {
            return state != State.REPLAY_CONFIRMED;
        }
    }
}
