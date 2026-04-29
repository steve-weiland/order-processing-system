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

### V1 baseline

V1 chaos run (`chaos-test/target/lag-v1.txt`):

```
burst=500 window=2s lag=500 store=33
```

Within a 2-second observation window, 33 records had been processed; lag
across the consumer group was the full 500.

### V2 status

**V2 does not address F5.** Throughput and correctness are different axes;
V2's charter was correctness. The same single-thread / 50 ms-per-record
shape is preserved.

### V2 number

V2 chaos run (`chaos-test/target/lag-v2.txt`, captured during the most
recent suite execution):

```
burst=500 window=2s lag=469
```

A few records *less* lag than V1 because V2 commits per-record (slightly
more overhead) but still in the same order of magnitude. The V1 assertion
(`lag > 200`) is preserved in V2.

### Evidence — assertion unchanged

```java
assertTrue(lag > 200, "V2 keeps the V1 baseline assertion. V3 will invert.");
```

```
[INFO] Tests run: 1 in F5_ConsumerLagTest    Time: 2.5 s    PASS
```

### V3 plan

- Increase `max.poll.records` and parallelize processing inside the
  consumer (per-partition virtual-thread workers, or
  [Confluent's parallel consumer](https://github.com/confluentinc/parallel-consumer))
- Re-run with the same 500-record burst; expect lag near zero
- Invert the F5 assertion to `lag < 50` and capture `lag-v3.txt`

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

## Summary table

| # | V1 result | V2 result | Mechanism | Test |
|---|-----------|-----------|-----------|------|
| F1 | 2 events | 1 event | `processed_orders` + `Idempotency-Key` | `F1_DuplicateOrdersTest` |
| F2 | 2 events | 1 event | `commitSync` after DB tx + idempotency | `F2_DuplicateNotificationsTest` |
| F3 | 0 events (lost) | 1 event | DB tx is the durability gate | `F3_LostOrdersTest` |
| F4 | consumer crashed, partition stalled | consumer survived, DLQ has 1, valid event = 1 | `byte[]` deserialize + DLQ routing | `F4_PoisonMessageTest` |
| F5 | lag 500 / 2s | lag 469 / 2s — **unchanged** | (V3) | `F5_ConsumerLagTest` |

V2 chaos suite total runtime: **~25 s** (Testcontainers spin-up + 5 tests).

---

## Known V2 limitations (deferred to V3)

- **Relay-crash duplicate-publish window.** The outbox relay is at-least-once.
  If the JVM crashes between `producer.send().get()` and
  `UPDATE outbox SET published_at = now()`, the next loop republishes — and
  notification-service has no idempotency. One duplicate notification per
  relay crash. Closes with notification-side dedup *(V3)*.
- **No retry-before-DLQ.** First parse failure routes straight to DLQ. Fine
  for malformed JSON; less fine for transient downstream failures *(V3)*.
- **Single-instance fulfillment.** Multiple replicas would race on
  `outbox` polling. Either single-leader election or `SELECT FOR UPDATE
  SKIP LOCKED` on the outbox poll *(V3)*.
- **F5 lag.** Documented above.
