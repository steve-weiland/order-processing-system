package com.steveweiland.orders.fulfillment;

import com.steveweiland.orders.common.Order;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class FulfillmentStore {
    private final Map<String, Order> store = new ConcurrentHashMap<>();

    public void put(Order order) {
        store.put(order.orderId(), order);
    }

    public int size() {
        return store.size();
    }
}
