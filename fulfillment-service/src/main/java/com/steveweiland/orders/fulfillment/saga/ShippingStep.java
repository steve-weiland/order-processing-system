package com.steveweiland.orders.fulfillment.saga;

import com.steveweiland.orders.common.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public final class ShippingStep implements SagaStep {
    private static final Logger log = LoggerFactory.getLogger(ShippingStep.class);

    private final double failureRate;
    private final Map<String, Boolean> shipments = new ConcurrentHashMap<>();
    private final AtomicInteger executions = new AtomicInteger();
    private final AtomicInteger compensations = new AtomicInteger();

    public ShippingStep(double failureRate) {
        this.failureRate = failureRate;
    }

    @Override
    public String name() { return "shipping"; }

    @Override
    public void execute(Order order) throws StepFailedException {
        executions.incrementAndGet();
        sleep(10);
        if (shouldFail()) {
            throw new StepFailedException(name(), "carrier unavailable (simulated)");
        }
        shipments.put(order.orderId(), Boolean.TRUE);
        log.info("shipment scheduled");
    }

    @Override
    public void compensate(Order order) {
        compensations.incrementAndGet();
        sleep(5);
        shipments.remove(order.orderId());
        log.info("shipment cancelled");
    }

    private boolean shouldFail() {
        return failureRate > 0.0 && ThreadLocalRandom.current().nextDouble() < failureRate;
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    public boolean scheduled(String orderId) {
        return shipments.containsKey(orderId);
    }

    public int executionCount() { return executions.get(); }
    public int compensationCount() { return compensations.get(); }
}
