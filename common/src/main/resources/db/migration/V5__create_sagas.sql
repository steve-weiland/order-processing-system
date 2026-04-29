CREATE TABLE sagas (
    saga_id            UUID         PRIMARY KEY,
    order_id           UUID         NOT NULL UNIQUE,
    state              TEXT         NOT NULL,
    payment_done_at    TIMESTAMPTZ,
    inventory_done_at  TIMESTAMPTZ,
    shipping_done_at   TIMESTAMPTZ,
    failure_step       TEXT,
    failure_reason     TEXT,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX sagas_state_idx ON sagas (state)
    WHERE state NOT IN ('COMPLETED', 'FAILED');
