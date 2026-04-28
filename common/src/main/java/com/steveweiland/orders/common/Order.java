package com.steveweiland.orders.common;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record Order(
        String orderId,
        String customerId,
        List<OrderItem> items,
        BigDecimal total,
        Instant placedAt) {}
