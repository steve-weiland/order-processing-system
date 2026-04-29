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

    public void insert(Connection c, String orderId, String customerId, Instant fulfilledAt) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO processed_orders (order_id, customer_id, fulfilled_at) " +
                        "VALUES (?::uuid, ?, ?) ON CONFLICT (order_id) DO NOTHING")) {
            ps.setString(1, orderId);
            ps.setString(2, customerId);
            ps.setTimestamp(3, Timestamp.from(fulfilledAt));
            ps.executeUpdate();
        }
    }
}
