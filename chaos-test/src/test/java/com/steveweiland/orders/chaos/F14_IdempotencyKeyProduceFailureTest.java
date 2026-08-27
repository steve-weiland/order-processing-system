package com.steveweiland.orders.chaos;

import com.steveweiland.orders.api.IdempotencyKeyStore;
import com.steveweiland.orders.api.OrderRequest;
import com.steveweiland.orders.api.OrderService;
import com.steveweiland.orders.common.Order;
import com.steveweiland.orders.common.OrderItem;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F14 — A produce failure poisons an Idempotency-Key. (spec.md §6 F14)
 *
 * The pre-fix flow committed the key→orderId row BEFORE producing to Kafka
 * (OPS-103 as originally written). If the produce then failed, the client got
 * a 500 — but the retry with the same key (the whole point of the header) hit
 * the replay path and received 202 + an orderId that never reached Kafka and
 * never would. A lost order reported as success, on the designed retry path.
 *
 * The fix under test: two-phase claim. A key is PENDING from claim until the
 * broker ack, then CONFIRMED. Replaying a PENDING key re-produces with the
 * stored orderId (duplicates on `orders` are absorbed downstream); replaying
 * a CONFIRMED key returns the stored orderId without producing.
 */
@Tag("chaos")
class F14_IdempotencyKeyProduceFailureTest extends KafkaTestFixture {

    @Test
    void retryAfterProduceFailureReproducesInsteadOfReplayingALie() throws Exception {
        IdempotencyKeyStore store = new IdempotencyKeyStore(dataSource());
        FailFirstPublisher publisher = new FailFirstPublisher(bootstrap(), orderTopic);
        OrderService service = new OrderService(publisher, store);
        String key = UUID.randomUUID().toString();
        OrderRequest req = sampleRequest("cust-f14");

        // Attempt 1: broker "down" — produce throws AFTER the key is claimed.
        assertThrows(Exception.class, () -> service.createOrder(req, key),
                "produce failure propagates to a 500 at the HTTP layer");
        String claimedOrderId = keyColumn(key, "order_id::text");
        assertNotNull(claimedOrderId, "key was claimed before the produce");
        assertNull(keyColumn(key, "confirmed_at::text"),
                "REGRESSION GUARD: pre-fix code had no pending state — the key was already 'confirmed'");
        assertEquals(0, drainOrders().size(), "nothing reached Kafka on the failed attempt");

        // Attempt 2: client retries with the same key. Pre-fix this returned
        // 202 + claimedOrderId with NOTHING on the topic, ever.
        OrderService.Result retry = service.createOrder(req, key);
        assertTrue(retry.produced(), "pending replay re-produces instead of replaying a lie");
        assertEquals(claimedOrderId, retry.orderId(), "the orderId claimed on attempt 1 is kept");
        assertNotNull(keyColumn(key, "confirmed_at::text"), "key confirmed after the broker ack");

        List<Order> produced = drainOrders();
        assertEquals(1, produced.size(), "exactly one record on the topic after the retry");
        assertEquals(claimedOrderId, produced.get(0).orderId());

        // Attempt 3: confirmed replay — same orderId, no new record.
        OrderService.Result replay = service.createOrder(req, key);
        assertFalse(replay.produced(), "confirmed key replays without producing");
        assertEquals(claimedOrderId, replay.orderId());
        assertEquals(1, drainOrders().size(), "still exactly one record on the topic");
        assertEquals(2, publisher.attempts(), "one failed produce + one successful re-produce");
    }

    private OrderRequest sampleRequest(String customerId) {
        return new OrderRequest(customerId,
                List.of(new OrderItem("SKU-A", 1, new BigDecimal("9.99"))));
    }

    private List<Order> drainOrders() {
        return TopicTailer.drain(bootstrap(), orderTopic, Order.class, Duration.ofSeconds(3));
    }

    private String keyColumn(String key, String column) throws Exception {
        try (Connection c = dataSource().getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT " + column + " FROM idempotency_keys WHERE key = ?")) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "idempotency_keys row exists for " + key);
                return rs.getString(1);
            }
        }
    }

    /** Real Kafka publisher that simulates a broker failure on the first call only. */
    private static final class FailFirstPublisher implements OrderService.Publisher {
        private final com.steveweiland.orders.api.OrderProducer delegate;
        private final AtomicInteger attempts = new AtomicInteger();

        FailFirstPublisher(String bootstrap, String topic) {
            this.delegate = new com.steveweiland.orders.api.OrderProducer(bootstrap, topic);
        }

        @Override
        public void publish(Order order) throws Exception {
            if (attempts.incrementAndGet() == 1) {
                throw new RuntimeException("simulated broker failure before ack");
            }
            delegate.send(order);
        }

        int attempts() { return attempts.get(); }
    }
}
