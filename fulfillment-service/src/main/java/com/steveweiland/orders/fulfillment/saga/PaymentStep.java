package com.steveweiland.orders.fulfillment.saga;

import com.steveweiland.orders.common.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public final class PaymentStep implements SagaStep {
    private static final Logger log = LoggerFactory.getLogger(PaymentStep.class);

    private final double failureRate;
    private final Map<String, Boolean> charges = new ConcurrentHashMap<>();
    private final AtomicInteger executions = new AtomicInteger();
    private final AtomicInteger compensations = new AtomicInteger();

    public PaymentStep(double failureRate) {
        this.failureRate = failureRate;
    }

    @Override
    public String name() { return "payment"; }

    @Override
    public void execute(Order order) throws StepFailedException {
        executions.incrementAndGet();
        sleep(10);
        if (shouldFail()) {
            throw new StepFailedException(name(), "payment declined (simulated)");
        }
        charges.put(order.orderId(), Boolean.TRUE);
        log.info("payment charged total={}", order.total());
    }

    @Override
    public void compensate(Order order) {
        compensations.incrementAndGet();
        sleep(5);
        charges.remove(order.orderId());
        log.info("payment refunded");
    }

    private boolean shouldFail() {
        return failureRate > 0.0 && ThreadLocalRandom.current().nextDouble() < failureRate;
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    /** For tests: was the payment compensated (refunded)? */
    public boolean charged(String orderId) {
        return charges.containsKey(orderId);
    }

    public int executionCount() { return executions.get(); }
    public int compensationCount() { return compensations.get(); }
}
