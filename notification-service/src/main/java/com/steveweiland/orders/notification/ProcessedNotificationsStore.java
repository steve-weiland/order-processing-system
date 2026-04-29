package com.steveweiland.orders.notification;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Atomic dedup gate for notifications. Closes the relay-crash duplicate-publish
 * window left open by v2.0.0–v2.2.0 — when the outbox relay crashes between a
 * successful Kafka publish and the {@code markPublishedBatch} UPDATE, the row
 * is republished on restart and {@code order-events} carries two records for
 * the same orderId. Without this store, notification-service logged the
 * customer twice; with it, the second consumer call to
 * {@link #claim(String)} returns {@code false} and the duplicate is skipped.
 */
public final class ProcessedNotificationsStore {
    private final DataSource ds;

    public ProcessedNotificationsStore(DataSource ds) {
        this.ds = ds;
    }

    /**
     * Returns {@code true} if this caller inserted the row (and should
     * therefore log the notification), {@code false} if the orderId was
     * already present (duplicate — skip the log).
     */
    public boolean claim(String orderId) {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO processed_notifications (order_id) VALUES (?::uuid) " +
                             "ON CONFLICT (order_id) DO NOTHING")) {
            ps.setString(1, orderId);
            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new RuntimeException("processed_notifications claim failed for orderId=" + orderId, e);
        }
    }
}
