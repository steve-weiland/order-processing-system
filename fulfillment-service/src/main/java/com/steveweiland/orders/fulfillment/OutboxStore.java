package com.steveweiland.orders.fulfillment;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public final class OutboxStore {
    private final DataSource ds;

    public OutboxStore(DataSource ds) {
        this.ds = ds;
    }

    public DataSource dataSource() {
        return ds;
    }

    public void insert(Connection c, String aggregateId, String topic, String recordKey, byte[] payload) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO outbox (aggregate_id, topic, record_key, payload) VALUES (?, ?, ?, ?)")) {
            ps.setString(1, aggregateId);
            ps.setString(2, topic);
            ps.setString(3, recordKey);
            ps.setBytes(4, payload);
            ps.executeUpdate();
        }
    }

    /**
     * Pick up to {@code limit} unpublished outbox rows and lock them for the
     * duration of the caller's transaction. Concurrent callers running in
     * separate transactions {@code SKIP LOCKED} rows another caller is
     * holding — disjoint work, no duplicate publishes.
     *
     * <p>The caller MUST hold the connection's transaction open until both the
     * Kafka publish and {@link #markPublishedBatch} have completed; releasing
     * the lock earlier reopens the duplicate-publish window. (See spec OPS-119.)
     */
    public List<Pending> findUnpublished(Connection c, int limit) throws SQLException {
        List<Pending> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT id, topic, record_key, payload FROM outbox " +
                        "WHERE published_at IS NULL ORDER BY id LIMIT ? " +
                        "FOR UPDATE SKIP LOCKED")) {
            ps.setInt(1, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new Pending(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBytes(4)));
                }
            }
        }
        return out;
    }

    public void markPublishedBatch(Connection c, List<Long> ids) throws SQLException {
        if (ids.isEmpty()) return;
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE outbox SET published_at = now() WHERE id = ANY(?)")) {
            Long[] arr = ids.toArray(Long[]::new);
            ps.setArray(1, c.createArrayOf("BIGINT", arr));
            ps.executeUpdate();
        }
    }

    public record Pending(long id, String topic, String recordKey, byte[] payload) {}
}
