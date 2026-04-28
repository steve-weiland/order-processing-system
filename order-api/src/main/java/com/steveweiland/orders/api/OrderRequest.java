package com.steveweiland.orders.api;

import com.steveweiland.orders.common.OrderItem;

import java.util.List;

public record OrderRequest(String customerId, List<OrderItem> items) {}
