package com.steveweiland.orders.api;

import com.steveweiland.orders.common.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Order-creation flow behind {@code POST /orders}, extracted from the HTTP
 * handler so the two-phase idempotency protocol (F14) is testable without
 * Javalin.
 *
 * <p>The ordering that matters: an {@code Idempotency-Key} is CLAIMED before
 * the produce (so a concurrent duplicate request replays instead of racing)
 * but CONFIRMED only after the broker ack. A produce failure leaves the key
 * pending; the client's retry re-produces with the stored orderId instead of
 * replaying an orderId that never reached Kafka. Re-producing is safe — a
 * duplicate record on {@code orders} is absorbed downstream (batch dedupe,
 * per-order saga lock, processed_orders).
 */
public final class OrderService {
    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    /** Publishes an order to the {@code orders} topic, blocking until the broker ack. */
    @FunctionalInterface
    public interface Publisher {
        void publish(Order order) throws Exception;
    }

    /** {@code produced} is false only for a confirmed-replay short-circuit. */
    public record Result(String orderId, boolean produced) {}

    private final Publisher publisher;
    private final IdempotencyKeyStore keys;

    public OrderService(Publisher publisher, IdempotencyKeyStore keys) {
        this.publisher = publisher;
        this.keys = keys;
    }

    /**
     * @param idempotencyKey optional; null or blank means no HTTP-layer dedup.
     * @throws Exception if the produce fails — a claimed key stays PENDING and
     *         the client's retry with the same key re-produces.
     */
    public Result createOrder(OrderRequest req, String idempotencyKey) throws Exception {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            Order order = buildOrder(req, UUID.randomUUID().toString());
            publisher.publish(order);
            return new Result(order.orderId(), true);
        }

        IdempotencyKeyStore.Decision dec = keys.claim(idempotencyKey, UUID.randomUUID().toString());
        if (!dec.mustProduce()) {
            log.info("idempotent replay key={} orderId={}", idempotencyKey, dec.orderId());
            return new Result(dec.orderId(), false);
        }
        if (dec.state() == IdempotencyKeyStore.State.REPLAY_PENDING) {
            log.info("pending key replay — re-producing key={} orderId={}", idempotencyKey, dec.orderId());
        }

        Order order = buildOrder(req, dec.orderId());
        publisher.publish(order);        // throws → key stays pending for the retry
        keys.confirm(idempotencyKey);
        return new Result(dec.orderId(), true);
    }

    private static Order buildOrder(OrderRequest req, String orderId) {
        BigDecimal total = OrderValidator.total(req);
        return new Order(orderId, req.customerId(), req.items(), total, Instant.now());
    }
}
