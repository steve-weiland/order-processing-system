CREATE TABLE processed_orders (
    order_id     UUID         PRIMARY KEY,
    customer_id  TEXT         NOT NULL,
    fulfilled_at TIMESTAMPTZ  NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
