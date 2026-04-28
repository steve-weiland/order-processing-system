package com.steveweiland.orders.common;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class JsonSerdeTest {

    @Test
    void orderRoundTrip() {
        Order original = new Order(
                "5f1c-uuid",
                "cust-123",
                List.of(new OrderItem("SKU-A", 2, new BigDecimal("9.99"))),
                new BigDecimal("19.98"),
                Instant.parse("2026-04-24T14:03:22.481Z"));

        JsonSerializer<Order> ser = new JsonSerializer<>();
        JsonDeserializer<Order> de = new JsonDeserializer<>(Order.class);

        byte[] bytes = ser.serialize("orders", original);
        Order roundTripped = de.deserialize("orders", bytes);

        assertEquals(original, roundTripped);
    }

    @Test
    void orderFulfilledRoundTrip() {
        OrderFulfilled original = new OrderFulfilled(
                "5f1c-uuid", "cust-123", Instant.parse("2026-04-24T14:03:22.534Z"));

        byte[] bytes = new JsonSerializer<OrderFulfilled>().serialize("order-events", original);
        OrderFulfilled roundTripped = new JsonDeserializer<>(OrderFulfilled.class)
                .deserialize("order-events", bytes);

        assertEquals(original, roundTripped);
    }

    @Test
    void nullSerializesToNullBytes() {
        assertNull(new JsonSerializer<Order>().serialize("orders", null));
        assertNull(new JsonDeserializer<>(Order.class).deserialize("orders", null));
    }
}
