CREATE TABLE idempotency_keys (
    key        TEXT         PRIMARY KEY,
    order_id   UUID         NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
