CREATE TABLE outbox (
    id            BIGSERIAL    PRIMARY KEY,
    aggregate_id  TEXT         NOT NULL,
    topic         TEXT         NOT NULL,
    record_key    TEXT         NOT NULL,
    payload       BYTEA        NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);

CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
