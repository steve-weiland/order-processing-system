CREATE TABLE processed_notifications (
    order_id    UUID         PRIMARY KEY,
    notified_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
