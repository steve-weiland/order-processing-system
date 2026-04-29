# Chaos Report — V1 → V2

> Companion to [README.md](../README.md) and [spec.md](../spec.md).
> What follows is the V1 → V2 transition recorded as five failure modes
> (F1–F5), each with the test that locked the V1 bug in place and the V2 fix
> that inverted it.

The premise of this build follows the [study plan](../../system-design-study-plan.md)
philosophy: *you understand a system when you have operated a buggy version of
it at 2am*. V1 was deliberately built broken in five well-known ways. The
chaos suite asserts the V1 brokenness — and the V2 implementation flips
each assertion. The git diff on those five test files is the load-bearing
portfolio artifact.

---

## Methodology

| Layer | Tool | Notes |
|-------|------|-------|
| Broker | `apache/kafka:3.8.1` (KRaft) | Same image as docker-compose; via `org.testcontainers.kafka.KafkaContainer` |
| Database | `postgres:16-alpine` | Flyway migrations applied once per JVM |
| Test runner | JUnit 5 + Awaitility | `@Tag("chaos")`, opt-in via `mvn -P chaos test` |
| Isolation | Per-test UUID-suffixed topics; `TRUNCATE` on shared DB tables before each test | Tests can run in parallel |
| Determinism | No timing races. Failures are reproduced via offset rewinds (`alterConsumerGroupOffsets`) and config flips (e.g. `enable.auto.commit=false`) rather than `Thread.sleep` |

Each failure mode is described in three parts:

1. **V1 bug** — what failed, and how the V1 test reproduced it deterministically
2. **V2 fix** — the requirement(s) implementing the fix, with code references
3. **Evidence** — the assertion change between V1 and V2 + the actual test output

---

## F1 — Duplicate orders

### V1 bug

A client retry on `POST /orders` (after a network timeout) put **two** records
on `orders` with the same `orderId`. V1 had no idempotency check, so the
fulfillment consumer processed both and the notification service logged two
notifications for one logical order.

### V1 test

```java
producer.send(order);   // first send
producer.send(order);   // client retry — same orderId
// ...
List<OrderFulfilled> events = TopicTailer.drain(...);
assertEquals(2, events.size());   // V1 baseline: PASSES because the bug is real
```

### V2 fix

Two layers:

- **HTTP layer** — optional `Idempotency-Key` header. `IdempotencyKeyStore.claim`
  uses Postgres `INSERT ... ON CONFLICT (key) DO NOTHING`. On replay, returns
  the original `orderId` and **does not produce** a Kafka record.
  *(Spec: OPS-17, OPS-103. Code: [`order-api/.../IdempotencyKeyStore.java`](../order-api/src/main/java/com/steveweiland/orders/api/IdempotencyKeyStore.java))*
- **Consumer layer** — `processed_orders` table; `FulfillmentConsumer` does
  `SELECT 1 FROM processed_orders WHERE order_id = ?::uuid` before any work.
  *(Spec: OPS-101. Code: [`fulfillment-service/.../ProcessedOrdersStore.java`](../fulfillment-service/src/main/java/com/steveweiland/orders/fulfillment/ProcessedOrdersStore.java))*

The two layers are intentionally redundant — they defend against different
failure modes (HTTP retry the broker never sees vs Kafka redelivery the
order-api never sees).

### Evidence

```diff
- assertEquals(2, events.size(), "V1 baseline: two notifications expected");
+ assertEquals(1, events.size(), "V2 fix: idempotency check absorbs the duplicate");
```

```
[INFO] Tests run: 1 in F1_DuplicateOrdersTest    Time: 4.9 s    PASS
```

---

## F2 — Duplicate notifications after consumer crash

### V1 bug

The V1 fulfillment consumer used `enable.auto.commit=true` with the default
5 s commit interval. If the consumer crashed *after* producing the
`OrderFulfilled` event but *before* the next auto-commit tick, on restart the
broker redelivered the original `orders` record → consumer fulfilled it again
→ **second** `OrderFulfilled` event → two notifications.

### V1 test mechanism

Rather than racing the 5 s tick, V1 reproduced the post-crash state
deterministically by setting `enable.auto.commit=false` for the first run
(no commit ever happens) and restarting with the same `group.id`:

```java
// First run — auto-commit suppressed; record processed; offset stays at 0
firstRun.stop(); // simulates crash before next tick
// Second run — same group.id, default config; record redelivered
List<OrderFulfilled> events = TopicTailer.drain(...);
assertEquals(2, events.size());   // V1: PASSES
```

### V2 fix

Two-part:

1. `enable.auto.commit=false` on the consumer; `commitSync` is called only
   after the DB transaction (state + outbox row) has committed.
   *(Spec: OPS-31. Code: [`FulfillmentConsumer.run`](../fulfillment-service/src/main/java/com/steveweiland/orders/fulfillment/FulfillmentConsumer.java))*
2. The `processed_orders` row inserted by the first run absorbs the
   redelivery on the second run — the idempotency check finds it and skips.
   *(Spec: OPS-101.)*

### V2 test mechanism

The first run now succeeds end-to-end (DB tx + outbox publish). The test
then **rewinds the consumer-group offset to 0** via
`AdminClient.alterConsumerGroupOffsets` to simulate "the V2 consumer crashed
before its `commitSync`". The second run sees the redelivered record, hits
the idempotency check, and skips. Net: still 1 event.

### Evidence

```diff
- assertEquals(2, events.size(), "V1 baseline: redelivery causes a second event");
+ assertEquals(1, events.size(), "V2 fix: idempotency check absorbs redelivery");
```

```
[INFO] Tests run: 1 in F2_DuplicateNotificationsTest    Time: 8.7 s    PASS
```

---

## F3 — Lost orders due to premature commit

### V1 bug

The opposite race from F2: auto-commit fired *after* `poll()` returned a
batch but *before* the consumer finished processing it. On crash + restart,
the offset was already past the unprocessed record — the order was silently
**lost**.

### V1 test mechanism

Used `AdminClient.alterConsumerGroupOffsets` to plant a committed offset
*past* the produced record before the consumer started, simulating "the
auto-commit fired and then we crashed":

```java
producer.send(order);
admin.alterConsumerGroupOffsets(groupId, offsetsAtLogEnd);
// consumer starts; resume offset is past the record
// → 0 events ever produced
assertEquals(0, events.size());   // V1: PASSES
```

### V2 fix

`commitSync` runs only after the DB tx has committed:

```java
// FulfillmentConsumer.run()
for (rec : batch) {
    processOne(rec);                        // includes DB tx commit
    consumer.commitSync(Map.of(             // only reached on success
        new TopicPartition(rec.topic(), rec.partition()),
        new OffsetAndMetadata(rec.offset() + 1)));
}
```

Durable state in Postgres is the gate; the offset cannot advance past it.
*(Spec: OPS-31, OPS-111.)*

### V2 test mechanism

The V1 "plant a bad offset" trick no longer reproduces a meaningful
failure — V2 doesn't have the auto-commit race in the first place. V2's
`F3_LostOrdersTest` therefore asserts the happy path: send 1 order, run V2
consumer, expect 1 event.

### Evidence

```diff
- assertEquals(0, events.size(), "V1 baseline: record skipped");
+ assertEquals(1, events.size(), "V2 fix: manual commitSync after DB tx → never lost");
```

```
[INFO] Tests run: 1 in F3_LostOrdersTest    Time: 2.3 s    PASS
```

---

## F4 — Poison message

### V1 bug

Records that failed to deserialize threw `SerializationException` out of
`KafkaConsumer.poll()`. V1 didn't catch it; the consumer thread terminated.
Worse, on restart the same record at the same offset crashed the consumer
again — the partition stalled forever.

### V1 test mechanism

Wrote malformed JSON via a raw `KafkaProducer<String, byte[]>`, then a valid
order with the same key (so same partition):

```java
raw.send(new ProducerRecord<>(orderTopic, poisonKey, "{not valid json".getBytes())).get();
producer.send(valid);
// consumer crashes on poison; valid record is never reached
Awaitility.await().until(consumer::crashed);
assertTrue(consumer.crashed());
assertEquals(0, validEvents.size());   // V1: PASSES
```

### V2 fix

Three changes to `FulfillmentConsumer`:

1. Deserialize values as `byte[]`, not as `Order`. JSON parsing happens
   inside `processOne` where it can be caught.
2. On parse failure, publish the original raw bytes to `orders.dlq` with
   diagnostic headers, then commit the source offset, then continue.
3. Add a `DlqProducer` keyed by source key (preserves partitioning).

*(Spec: OPS-32, OPS-121, OPS-122. Code:
[`FulfillmentConsumer.processOne`](../fulfillment-service/src/main/java/com/steveweiland/orders/fulfillment/FulfillmentConsumer.java),
[`DlqProducer`](../fulfillment-service/src/main/java/com/steveweiland/orders/fulfillment/DlqProducer.java))*

### Evidence — assertion flip

```diff
- assertTrue(consumer.crashed(), "V1: consumer must terminate on poison");
- assertEquals(0, events.size(), "V1: valid order behind poison is never fulfilled");
+ assertFalse(stack.consumer.crashed(), "V2 fix: consumer no longer crashes on poison");
+ assertEquals(1, events.size(), "V2 fix: valid order behind poison is fulfilled normally");
+ assertEquals(1, dlq.size(), "V2 fix: poison record routed to DLQ");
+ assertTrue(d.headers.containsKey("x-dlq-reason"));
+ assertEquals(orderTopic, d.headers.get("x-dlq-source-topic"));
```

### Evidence — actual DLQ payload

Captured during a manual `docker compose up` run with a poison message
injected via `kafka-console-producer`:

```
$ make consume-dlq
x-dlq-reason:JsonParseException: Unrecognized token 'not': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')
 at [Source: REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled); line: 1, column: 5],
x-dlq-source-topic:orders,
x-dlq-source-partition:2,
x-dlq-source-offset:1,
x-dlq-original-timestamp:1777391199602
not-valid-json

$ docker compose ps fulfillment-service
NAME              STATUS
ops-fulfillment   Up 50 seconds          # consumer survived

# pipeline still works:
$ curl -X POST http://localhost:6080/orders -H 'Content-Type: application/json' \
       -d '{"customerId":"cust-after-poison","items":[{"sku":"SKU-Z","quantity":1,"unitPrice":3.00}]}'
{"orderId":"4dff686d-..."}

$ make psql
> SELECT count(*) FROM processed_orders;
3                                          # poison didn't block the next order
```

```
[INFO] Tests run: 1 in F4_PoisonMessageTest    Time: 3.8 s    PASS
```

---

## F5 — Consumer lag under burst load

### V1 bug

A single consumer with 50 ms simulated work cannot keep up with a 500-record
burst. Lag grew linearly; notifications arrived minutes late.

### Baseline timeline

| Version | Lag at t=2 s (after 500-record burst) | Mechanism |
|---------|---------------------------------------|-----------|
| V1 | 500 (33 processed) | single-thread, auto-commit |
| v2.0.0 | 469 | single-thread, manual commit (slightly more per-record overhead) |
| **v2.1.0** | **0** | virtual-thread parallel batch + async outbox publish |

Real artifacts captured during chaos runs:

```
$ cat chaos-test/target/lag-v1.txt
burst=500 window=2s lag=500 store=33

$ cat chaos-test/target/lag-v2.1.txt
burst=500 window=2s lag=0
```

### v2.1.0 fix

Three changes in [`FulfillmentConsumer.java`](../fulfillment-service/src/main/java/com/steveweiland/orders/fulfillment/FulfillmentConsumer.java):

1. **Virtual-thread executor** — `Executors.newVirtualThreadPerTaskExecutor()`.
   Every record in a `poll()` batch is submitted as its own task. Java 21
   virtual threads don't pin OS threads on `Thread.sleep` or DB I/O.
2. **Bounded concurrency** — a `Semaphore` capped by `--worker-pool-size`
   (default 32) keeps in-flight work bounded so a pathological burst can't
   exhaust memory. The Hikari pool (16) is the secondary, lower bound.
3. **Per-batch commit** — `commitSync` runs once per batch with the highest
   successful offset per partition. If any record fails, *no* offset
   advances; the batch is replayed and the `processed_orders` idempotency
   check (now an atomic `INSERT … ON CONFLICT DO NOTHING`) absorbs records
   that already wrote durable state.

Plus one change in [`OutboxRelay.pollAndPublish`](../fulfillment-service/src/main/java/com/steveweiland/orders/fulfillment/OutboxRelay.java):

4. **Async batch publish** — the relay now issues every `producer.send()` in
   the polled batch *before* awaiting any future, then awaits each. After
   all acks, a single batched `UPDATE outbox SET published_at = now() WHERE
   id = ANY(?)` marks the whole batch published.

### A race we found and fixed

v2.0.0 used `SELECT exists() THEN INSERT processed_orders`. With v2.1.0
parallelism, two workers in the same poll batch (or split between consumer
and outbox-relay redelivery) could both pass the `SELECT`, both proceed to
the INSERT, and both write outbox rows for the same orderId — even though
the second INSERT was a no-op due to the PK constraint. F1 went red on the
first parallel run.

The fix is structural: collapse the check and the insert into a single
atomic statement and use the row count to decide.

```diff
- if (processedStore.exists(c, order.orderId())) {
-     c.commit(); return;
- }
- processedStore.insert(c, order.orderId(), order.customerId(), fulfilledAt);
- outboxStore.insert(c, …);
+ boolean claimed = processedStore.insert(c, order.orderId(),
+         order.customerId(), fulfilledAt);
+ if (!claimed) {
+     c.commit(); return;
+ }
+ outboxStore.insert(c, …);
```

The `INSERT` IS the check. There is no window between them.

### Evidence — assertion flip

```diff
- assertTrue(lag > 200, "v2.0.0 keeps the V1 baseline assertion. v2.1.0 will invert.");
+ assertTrue(lag < LAG_THRESHOLD,
+         "v2.1.0 fix: expected lag < " + LAG_THRESHOLD + ", got " + lag);
```

```
[INFO] Tests run: 1 in F5_ConsumerLagTest    Time: 3.0 s    PASS
```

---

## Idempotency-Key — manual demonstration

Captured during the same end-to-end run:

```
$ KEY=$(uuidgen)

$ curl -X POST localhost:6080/orders -H "Idempotency-Key: $KEY" \
       -d '{"customerId":"bob","items":[{"sku":"SKU-B","quantity":2,"unitPrice":2.50}]}'
{"orderId":"d8c019c6-aca7-492d-a441-d2a63f5753be"}

# replay — same key, same response, no second Kafka record
$ curl -X POST localhost:6080/orders -H "Idempotency-Key: $KEY" \
       -d '{"customerId":"bob","items":[{"sku":"SKU-B","quantity":2,"unitPrice":2.50}]}'
{"orderId":"d8c019c6-aca7-492d-a441-d2a63f5753be"}

$ make psql
> SELECT key, order_id FROM idempotency_keys;
                   key                    |               order_id
------------------------------------------+--------------------------------------
 <KEY>                                    | d8c019c6-aca7-492d-a441-d2a63f5753be
(1 row)

> SELECT order_id, customer_id FROM processed_orders WHERE customer_id='bob';
              order_id                | customer_id
--------------------------------------+-------------
 d8c019c6-aca7-492d-a441-d2a63f5753be | bob
(1 row)                                # exactly one fulfillment, despite two POSTs
```

---

## F6 — Multi-instance outbox-poll race

### When it manifests

F6 is **new in v2.2.0**. It didn't exist in V1 (no outbox at all), or in
v2.0.0 / v2.1.0 (single fulfillment-service JVM). The first time the
project runs two `fulfillment-service` instances against the same
Postgres, both relay threads poll the same
`SELECT id, … FROM outbox WHERE published_at IS NULL` and both pick up
identical rows. They each publish to Kafka; the `notification-service`
sees every event twice. Customers get two notifications per order.

### v2.2.0 fix

[`OutboxStore.findUnpublished`](../fulfillment-service/src/main/java/com/steveweiland/orders/fulfillment/OutboxStore.java)
adds `FOR UPDATE SKIP LOCKED`. [`OutboxRelay.pollAndPublish`](../fulfillment-service/src/main/java/com/steveweiland/orders/fulfillment/OutboxRelay.java)
wraps the entire poll → publish → mark cycle in a single Postgres
transaction so the row-level locks are held across the Kafka publish.

```diff
- public List<Pending> findUnpublished(int limit) { /* opens own connection */ }
+ public List<Pending> findUnpublished(Connection c, int limit) {
+     "SELECT … FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT ? " +
+     "FOR UPDATE SKIP LOCKED"
+ }
```

```diff
- // v2.1.0: send + mark, no shared transaction
- List<Pending> pending = store.findUnpublished(BATCH);
- producer.send(...).get(); store.markPublishedBatch(ids);
+ // v2.2.0: single tx; lock held across publish
+ try (Connection c = ds.getConnection()) {
+     c.setAutoCommit(false);
+     List<Pending> pending = store.findUnpublished(c, BATCH);
+     for (p : pending) futures.add(producer.send(...));
+     for (f : futures) f.get();
+     store.markPublishedBatch(c, ids);
+     c.commit();
+ }
```

### Evidence — chaos test

`F6_MultiInstanceOutboxRaceTest` spawns two `V2Stack`s sharing the same
broker + Postgres, sends 50 orders, asserts exactly 50 events on
`order-events`. The pre-fix expected behavior is 100 (every event
duplicated). Test runs in ~5.8 s.

```
[INFO] Tests run: 1 in F6_MultiInstanceOutboxRaceTest    Time: 5.8 s    PASS
```

### Evidence — manual run

A `docker compose up` with the v2.2.0 two-instance topology, then 100
orders POSTed in a tight loop:

```
$ docker logs ops-fulfillment-1 2>&1 | grep '"relay published count' | wc -l
4    # 4 ticks: count=15, 13, 2, ...
$ docker logs ops-fulfillment-2 2>&1 | grep '"relay published count' | wc -l
7    # 7 ticks: count=13, 11, 13, ...

$ docker exec ops-kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic order-events \
    | awk -F: '{sum+=$3} END {print sum}'
100   # exactly 100 events for 100 orders — no duplicates
```

Both relays did real work (publishing disjoint rows); the total event
count matches the order count exactly.

---

## F7 — Relay-crash duplicate-publish window

### When it manifests

The outbox relay holds a DB transaction across the Kafka publish so it
can commit `markPublishedBatch` atomically with the lock release. If the
JVM crashes **after** Kafka has acked the record but **before** the
relay's `commit()` lands, the DB tx rolls back — `published_at` stays
NULL — so the next relay tick (this instance on restart, or another
instance) picks up the row and republishes. Kafka's idempotent producer
dedupes only within a single producer session; across a crash, the new
session emits the record again. `order-events` ends up with two records
for the same orderId.

In v2.0.0–v2.2.0, `notification-service` had no idempotency, so each
duplicate event triggered a duplicate notification. F7 was a documented
known limitation across that whole window.

### v2.3.0 fix

A `processed_notifications` table:

```sql
CREATE TABLE processed_notifications (
    order_id    UUID         PRIMARY KEY,
    notified_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
```

[`NotificationConsumer.notifyOne`](../notification-service/src/main/java/com/steveweiland/orders/notification/NotificationConsumer.java)
now claims the orderId before logging:

```java
if (!store.claim(event.orderId())) {
    log.info("duplicate notification suppressed partition={} offset={}", partition, offset);
    return;
}
log.info("Notification sent for order={} customer={} ...");
hook.accept(event);
```

`ProcessedNotificationsStore.claim` is the same atomic
`INSERT … ON CONFLICT DO NOTHING` pattern that `processed_orders` uses;
row count = 1 means we won the race, 0 means a previous delivery already
notified this customer.

### Evidence — chaos test

`F7_NotificationDedupTest` publishes two identical `OrderFulfilled`
records directly to `order-events`, then asserts the in-memory hook fires
exactly once. ~3 s.

```
[INFO] Tests run: 1 in F7_NotificationDedupTest    Time: 3.2 s    PASS
```

### Evidence — manual run

`docker compose up`; POST 5 orders; manually inject a duplicate
`OrderFulfilled` for one of them via `kafka-console-producer`:

```
$ docker exec -i ops-kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic order-events <<< \
    '{"orderId":"00778798-…","customerId":"replay","fulfilledAt":"2026-04-29T12:00:00Z"}'

$ docker logs ops-notifications | grep -E '(Notification sent|duplicate notification)'
…  Notification sent for order=00778798-… customer=v23-1 partition=2 offset=0
…  Notification sent for order=5b46cfd3-… customer=v23-3 partition=1 offset=0
…  Notification sent for order=b49a0cc4-… customer=v23-2 partition=0 offset=0
…  Notification sent for order=eaf81015-… customer=v23-5 partition=0 offset=1
…  Notification sent for order=e5dcd42b-… customer=v23-4 partition=0 offset=2
…  duplicate notification suppressed partition=1 offset=1   ← injected duplicate

$ docker exec ops-postgres psql -U orders -d orders -t -c \
    'SELECT count(*) FROM processed_notifications;'
   5    ← still 5; duplicate absorbed
```

---

## F8 — Payment step fails

V3.0.0 introduces the saga state machine. Payment is the first step; if it
fails, no prior steps need compensation, the saga goes directly to `FAILED`,
and `processed_orders` is inserted for idempotency but no outbox row is
written.

`F8_PaymentFailureNoCompensationTest` runs in ~0.1 s — pure orchestrator + DB,
no Kafka.

```
sagaState                 = FAILED
failure_step              = payment
payment.executionCount    = 1
payment.compensationCount = 0    ← no compensation needed
inventory.executionCount  = 0    ← never reached
```

## F9 — Inventory fails after payment

Compensation runs in reverse order: `payment.compensate()` refunds before
the saga transitions to `FAILED`.

```
sagaState                  = FAILED
failure_step               = inventory
payment.executionCount     = 1
payment.compensationCount  = 1   ← refunded
inventory.executionCount   = 1   ← attempted
inventory.compensationCount = 0
shipping.executionCount    = 0   ← never reached
```

## F10 — Shipping fails after payment + inventory

Both prior steps compensated, in reverse order (inventory first, then payment).

```
shipping.compensationCount  = 0   ← shipping never succeeded
inventory.compensationCount = 1   ← released first
payment.compensationCount   = 1   ← refunded last
```

## F11 — Consumer crashes mid-saga

The contract under test is **state-aware re-entry**: every state transition
is persisted before the next step runs, so on redelivery the orchestrator
resumes at the next pending step without re-executing completed work.

The test wraps `InventoryStep` with one that throws `RuntimeException` on
first call (simulating a JVM crash). After the first run propagates the
exception out, the saga row sits at `INVENTORY_PENDING` (the
`PAYMENT_PENDING → INVENTORY_PENDING` transition was already committed). A
second `SagaOrchestrator` instance over the same `SagaStore` runs against
the same in-memory step state — it finds the existing saga row, skips the
payment block, runs inventory + shipping, reaches `COMPLETED`.

```
After two invocations:
  payment.executionCount    = 1   ← NOT re-executed on resume
  inventory.executionCount  = 1   ← ran on resume only
  shipping.executionCount   = 1   ← ran on resume only
  payment.compensationCount = 0   ← saga succeeded; no compensation
```

## Manual run — failed-saga path end-to-end

```
$ docker compose up -d --build
$ docker compose stop fulfillment-service-2
$ docker compose run -d --rm \
    -e INVENTORY_FAILURE_RATE=1.0 \
    --name ops-fulfillment-failing fulfillment-service-1
$ for i in 1 2; do curl -s -X POST localhost:6080/orders … ; done

$ docker exec ops-postgres psql -U orders -d orders -c \
    "SELECT order_id, state, failure_step, failure_reason FROM sagas WHERE state='FAILED';"
              order_id                | state  | failure_step |          failure_reason
--------------------------------------+--------+--------------+----------------------------------
 221f60d2-006b-419b-9d6d-903bf5a7f6fc | FAILED | inventory    | inventory unavailable (simulated)
 4949ddc7-a1ee-433f-a098-8ad253a3b0ef | FAILED | inventory    | inventory unavailable (simulated)

$ docker logs ops-fulfillment-failing | grep 'payment refunded'
…  payment refunded   orderId=221f60d2-…
…  payment refunded   orderId=4949ddc7-…

$ docker exec ops-kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic order-events \
    | awk -F: '{sum+=$3} END {print sum}'
3   ← unchanged from happy-path baseline; failed sagas emit no event
```

---

## Summary table

| # | Result | Mechanism | Test |
|---|--------|-----------|------|
| F1 | 1 event for duplicate sends | `processed_orders` + `Idempotency-Key` (v2.0.0) | `F1_DuplicateOrdersTest` |
| F2 | 1 event after redelivery | `commitSync` after DB tx + idempotency (v2.0.0) | `F2_DuplicateNotificationsTest` |
| F3 | record never lost | DB tx is the durability gate (v2.0.0) | `F3_LostOrdersTest` |
| F4 | poison → DLQ + consumer survives | `byte[]` deserialize + DLQ routing (v2.0.0) | `F4_PoisonMessageTest` |
| F5 | lag 500 → 0 / 2s window | virtual-thread parallel batch + async outbox (v2.1.0) | `F5_ConsumerLagTest` |
| F6 | exactly N events for N orders, 2 relays | `SELECT FOR UPDATE SKIP LOCKED` (v2.2.0) | `F6_MultiInstanceOutboxRaceTest` |
| F7 | 1 notification per orderId | `processed_notifications` (v2.3.0) | `F7_NotificationDedupTest` |
| F8 | saga `FAILED`, no compensation | saga state machine (v3.0.0) | `F8_PaymentFailureNoCompensationTest` |
| F9 | inventory failure → payment refunded | reverse-order compensation (v3.0.0) | `F9_InventoryFailureCompensatesPaymentTest` |
| F10 | shipping failure → both prior compensated | reverse-order compensation (v3.0.0) | `F10_ShippingFailureCompensatesAllTest` |
| F11 | mid-saga crash → resume at next step | state-aware re-entry (v3.0.0) | `F11_SagaResumeAfterCrashTest` |

V2 chaos suite total runtime: **~25 s** (Testcontainers spin-up + 5 tests).

---

## Known v2.x limitations (deferred)

- **No retry-before-DLQ** *(v2.4.0+ if needed)*. First parse failure
  routes straight to DLQ. Fine for malformed JSON (deterministic — retries
  won't help). Originally targeted for v2.3.0 but deferred: there's no
  concrete transient-error path in the current pipeline that retry-topics
  would mitigate. Will revisit when an external HTTP call or similar
  flaky dependency lands.
