package com.steveweiland.orders.fulfillment;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;

public final class ProcessedOrdersStore {

    /**
     * Terminal outcome recorded per order. The table answers "has this order
     * been processed" for idempotency in BOTH cases; only FULFILLED rows
     * represent an order the customer will receive (and carry fulfilled_at).
     */
    public enum Status { FULFILLED, FAILED }

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
     *
     * @param fulfilledAt required for {@link Status#FULFILLED}, must be null
     *                    for {@link Status#FAILED} — a failed order has no
     *                    fulfillment time.
     */
    public boolean insert(Connection c, String orderId, String customerId,
                          Status status, Instant fulfilledAt) throws SQLException {
        if ((status == Status.FULFILLED) == (fulfilledAt == null)) {
            throw new IllegalArgumentException(
                    "fulfilledAt must be set exactly when status is FULFILLED; got "
                            + status + " with fulfilledAt=" + fulfilledAt);
        }
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO processed_orders (order_id, customer_id, status, fulfilled_at) " +
                        "VALUES (?::uuid, ?, ?, ?) ON CONFLICT (order_id) DO NOTHING")) {
            ps.setString(1, orderId);
            ps.setString(2, customerId);
            ps.setString(3, status.name());
            if (fulfilledAt == null) {
                ps.setNull(4, Types.TIMESTAMP_WITH_TIMEZONE);
            } else {
                ps.setTimestamp(4, Timestamp.from(fulfilledAt));
            }
            return ps.executeUpdate() == 1;
        }
    }
}
