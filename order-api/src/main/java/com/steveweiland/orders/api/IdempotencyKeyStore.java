package com.steveweiland.orders.api;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Postgres-backed store for the optional {@code Idempotency-Key} HTTP header.
 * <p>
 * On {@link #claim} the store atomically:
 * <ol>
 *   <li>Attempts to insert {@code (key, generatedOrderId)} via {@code ON CONFLICT DO NOTHING}.</li>
 *   <li>If the row is brand-new, returns {@link Decision#fresh}.</li>
 *   <li>If the row already existed (replay or race-loser), looks up the original
 *       {@code order_id} and returns {@link Decision#replay}.</li>
 * </ol>
 * Callers SHOULD NOT produce a Kafka record on a replay decision.
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
                    return Decision.fresh(generatedOrderId);
                }
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT order_id::text FROM idempotency_keys WHERE key = ?")) {
                ps.setString(1, key);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return Decision.replay(rs.getString(1));
                    }
                }
            }
            throw new IllegalStateException("idempotency_keys lost a row we just inserted: key=" + key);
        } catch (SQLException e) {
            throw new RuntimeException("idempotency claim failed", e);
        }
    }

    public record Decision(String orderId, boolean isReplay) {
        public static Decision fresh(String id) { return new Decision(id, false); }
        public static Decision replay(String id) { return new Decision(id, true); }
    }
}
