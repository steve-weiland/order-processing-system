-- Two-phase Idempotency-Key claim (F14). A key row is PENDING (confirmed_at
-- IS NULL) from claim until the Kafka produce is acked, then CONFIRMED.
-- Replaying a CONFIRMED key returns the stored orderId without producing;
-- replaying a PENDING key means a previous attempt claimed the key but its
-- produce never succeeded — the retry re-produces with the stored orderId
-- (duplicates on `orders` are absorbed downstream) and then confirms.
--
-- Before this, the key row committed BEFORE the produce: a produce failure
-- left a poisoned key whose replay returned 202 + an orderId that never
-- reached Kafka — a lost order reported as success, on the designed retry
-- path.
ALTER TABLE idempotency_keys ADD COLUMN confirmed_at TIMESTAMPTZ;

-- Backfill: pre-V6 rows carry no produce-outcome evidence; treat them as
-- confirmed rather than re-producing months-old orders on a stray replay.
UPDATE idempotency_keys SET confirmed_at = created_at WHERE confirmed_at IS NULL;
