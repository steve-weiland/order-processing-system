package com.steveweiland.orders.fulfillment;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

public final class ProcessedOrdersStore {

    public boolean exists(Connection c, String orderId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT 1 FROM processed_orders WHERE order_id = ?::uuid")) {
            ps.setString(1, orderId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    /**
     * Atomically claim the orderId. Returns {@code true} if this caller
     * inserted the row (and therefore "owns" the fulfillment), {@code false}
     * if another transaction had already inserted it. The check-then-insert
     * race that v2.0.0's separate {@link #exists} could miss under v2.1.0
     * parallel batch processing is closed by the {@code ON CONFLICT … DO
     * NOTHING}: only one of N concurrent workers receives a row count of 1.
     */
    public boolean insert(Connection c, String orderId, String customerId, Instant fulfilledAt) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO processed_orders (order_id, customer_id, fulfilled_at) " +
                        "VALUES (?::uuid, ?, ?) ON CONFLICT (order_id) DO NOTHING")) {
            ps.setString(1, orderId);
            ps.setString(2, customerId);
            ps.setTimestamp(3, Timestamp.from(fulfilledAt));
            return ps.executeUpdate() == 1;
        }
    }
}
