package com.steveweiland.orders.fulfillment.saga;

import com.steveweiland.orders.common.Order;

public interface SagaStep {
    String name();

    void execute(Order order) throws StepFailedException;

    void compensate(Order order);
}
