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

    public List<Pending> findUnpublished(int limit) {
        List<Pending> out = new ArrayList<>();
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT id, topic, record_key, payload FROM outbox " +
                             "WHERE published_at IS NULL ORDER BY id LIMIT ?")) {
            ps.setInt(1, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new Pending(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getBytes(4)));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("outbox scan failed", e);
        }
        return out;
    }

    public void markPublished(long id) {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "UPDATE outbox SET published_at = now() WHERE id = ?")) {
            ps.setLong(1, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("outbox mark-published failed for id=" + id, e);
        }
    }

    public record Pending(long id, String topic, String recordKey, byte[] payload) {}
}
