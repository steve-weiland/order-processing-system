package com.steveweiland.orders.common;

import java.time.Instant;

public record OrderFulfilled(String orderId, String customerId, Instant fulfilledAt) {}
