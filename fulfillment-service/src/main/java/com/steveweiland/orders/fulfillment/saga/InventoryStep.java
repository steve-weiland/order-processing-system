package com.steveweiland.orders.fulfillment.saga;

import com.steveweiland.orders.common.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public final class InventoryStep implements SagaStep {
    private static final Logger log = LoggerFactory.getLogger(InventoryStep.class);

    private final double failureRate;
    private final Map<String, Boolean> reservations = new ConcurrentHashMap<>();
    private final AtomicInteger executions = new AtomicInteger();
    private final AtomicInteger compensations = new AtomicInteger();

    public InventoryStep(double failureRate) {
        this.failureRate = failureRate;
    }

    @Override
    public String name() { return "inventory"; }

    @Override
    public void execute(Order order) throws StepFailedException {
        executions.incrementAndGet();
        sleep(10);
        if (shouldFail()) {
            throw new StepFailedException(name(), "inventory unavailable (simulated)");
        }
        reservations.put(order.orderId(), Boolean.TRUE);
        log.info("inventory reserved items={}", order.items().size());
    }

    @Override
    public void compensate(Order order) {
        compensations.incrementAndGet();
        sleep(5);
        reservations.remove(order.orderId());
        log.info("inventory released");
    }

    private boolean shouldFail() {
        return failureRate > 0.0 && ThreadLocalRandom.current().nextDouble() < failureRate;
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    public boolean reserved(String orderId) {
        return reservations.containsKey(orderId);
    }

    public int executionCount() { return executions.get(); }
    public int compensationCount() { return compensations.get(); }
}
