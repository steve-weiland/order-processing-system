package com.steveweiland.orders.api;

import com.steveweiland.orders.common.OrderItem;

import java.math.BigDecimal;

public final class OrderValidator {
    private OrderValidator() {}

    public static String validate(OrderRequest req) {
        if (req == null) return "request body is required";
        if (req.customerId() == null || req.customerId().isBlank()) {
            return "customerId is required";
        }
        if (req.items() == null || req.items().isEmpty()) {
            return "items array is required and non-empty";
        }
        for (int i = 0; i < req.items().size(); i++) {
            OrderItem it = req.items().get(i);
            if (it == null) return "items[" + i + "] is null";
            if (it.sku() == null || it.sku().isBlank()) return "items[" + i + "].sku is required";
            if (it.quantity() <= 0) return "items[" + i + "].quantity must be > 0";
            if (it.unitPrice() == null || it.unitPrice().compareTo(BigDecimal.ZERO) < 0) {
                return "items[" + i + "].unitPrice must be >= 0";
            }
        }
        return null;
    }

    public static BigDecimal total(OrderRequest req) {
        BigDecimal total = BigDecimal.ZERO;
        for (OrderItem it : req.items()) {
            total = total.add(it.unitPrice().multiply(BigDecimal.valueOf(it.quantity())));
        }
        return total;
    }
}
