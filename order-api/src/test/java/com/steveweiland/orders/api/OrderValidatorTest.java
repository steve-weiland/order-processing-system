package com.steveweiland.orders.api;

import com.steveweiland.orders.common.OrderItem;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class OrderValidatorTest {

    @Test
    void acceptsMinimalValidOrder() {
        OrderRequest req = new OrderRequest("c-1", List.of(
                new OrderItem("SKU-A", 1, new BigDecimal("1.00"))));
        assertNull(OrderValidator.validate(req));
    }

    @Test
    void rejectsBlankCustomerId() {
        OrderRequest req = new OrderRequest(" ", List.of(
                new OrderItem("SKU-A", 1, BigDecimal.ONE)));
        assertNotNull(OrderValidator.validate(req));
    }

    @Test
    void rejectsEmptyItems() {
        OrderRequest req = new OrderRequest("c-1", List.of());
        assertNotNull(OrderValidator.validate(req));
    }

    @Test
    void rejectsZeroQuantity() {
        OrderRequest req = new OrderRequest("c-1", List.of(
                new OrderItem("SKU-A", 0, BigDecimal.ONE)));
        assertNotNull(OrderValidator.validate(req));
    }

    @Test
    void rejectsNegativeUnitPrice() {
        OrderRequest req = new OrderRequest("c-1", List.of(
                new OrderItem("SKU-A", 1, new BigDecimal("-1.00"))));
        assertNotNull(OrderValidator.validate(req));
    }

    @Test
    void computesTotal() {
        OrderRequest req = new OrderRequest("c-1", List.of(
                new OrderItem("SKU-A", 2, new BigDecimal("9.99")),
                new OrderItem("SKU-B", 1, new BigDecimal("4.50"))));
        assertEquals(new BigDecimal("24.48"), OrderValidator.total(req));
    }
}
