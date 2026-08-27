-- processed_orders records "this order reached a terminal state", not
-- "this order was fulfilled" — failed sagas insert a row too (idempotency).
-- Pre-V7 the row shape lied about that: a FAILED saga got fulfilled_at =
-- now(), so the README's "fulfilled orders" query returned failed orders.
-- status distinguishes the outcomes; fulfilled_at is set only on FULFILLED.
ALTER TABLE processed_orders ADD COLUMN status TEXT NOT NULL DEFAULT 'FULFILLED';
ALTER TABLE processed_orders ALTER COLUMN fulfilled_at DROP NOT NULL;
