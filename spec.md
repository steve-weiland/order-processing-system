# Event-Driven Order Processing System — V2

| Field   | Value              |
|---------|--------------------|
| Version | 0.2 (draft)        |
| Author  | Steve Weiland      |
| Date    | 2026-04-28         |
| Status  | Draft              |

---

## 1. Overview

V2 fixes the four correctness failures documented in V1 (F1–F4) using three
mechanisms:

1. **Idempotency** — a DB-backed `processed_orders` table is consulted before
   any fulfillment work. Duplicate orders (whether produced by HTTP retries or
   Kafka redelivery) are detected and skipped. The HTTP layer additionally
   supports a Stripe-style `Idempotency-Key` header so retries on `POST /orders`
   return the original `orderId` instead of creating a new order.

2. **Transactional outbox** — fulfillment writes its state (`processed_orders`)
   and its event payload (`outbox`) in a single Postgres transaction. A relay
   thread inside `fulfillment-service` polls the outbox and publishes to
   Kafka with an idempotent producer, marking each row published only after
   the broker acks. Kafka consumer offsets are committed manually with
   `commitSync` *after* the DB transaction succeeds, eliminating the V1
   auto-commit races that caused F2 (duplicate notifications) and F3 (lost
   orders).

3. **Dead letter queue** — the fulfillment consumer reads as `byte[]` and
   parses JSON manually. On parse failure the raw record is republished to
   `orders.dlq` with diagnostic headers, the source offset is committed, and
   processing continues. The poison record no longer stalls the partition (F4).

V2 is correctness-focused. **F5 (consumer lag under burst load) is deferred
to v2.1.0** — throughput tuning, parallel consumers, and `max.poll.records`
sizing are a separate axis from the durability and ordering work in v2.0.0.

---

## 2. Definitions

| Term | Definition |
|------|------------|
| Producer | A client that writes records to a Kafka topic |
| Consumer | A client that reads records from a Kafka topic |
| Topic | A named, partitioned, append-only log of records |
| Partition | An ordered subset of a topic; unit of parallelism and ordering |
| Offset | The position of a record within a partition |
| Consumer group | A set of consumers cooperatively consuming a topic |
| Auto-commit | Consumer config that commits offsets on a background timer regardless of processing state. **Disabled in V2.** |
| Manual commit | Application calls `commitSync` after work succeeds. **V2 default.** |
| At-most-once | Delivery semantics in which a record may be lost but never duplicated |
| At-least-once | Delivery semantics in which a record may be duplicated but never lost |
| Effectively-once | At-least-once delivery + idempotent processing → each effect happens exactly once |
| Idempotency key | A unique identifier used to detect and skip duplicate processing of the same logical operation |
| `Idempotency-Key` header | HTTP header carrying a client-supplied idempotency key on `POST /orders` |
| `processed_orders` table | Postgres table mapping `orderId → fulfillment state`; the durable record of "this order has been processed" |
| Outbox table | Postgres table holding event payloads waiting to be published to Kafka |
| Outbox relay | Background thread that reads unpublished outbox rows, publishes to Kafka, and marks them published |
| Idempotent producer | Kafka producer with `enable.idempotence=true`; deduplicates retries within a single session via per-partition sequence numbers |
| Dead letter queue (DLQ) | Topic that receives records the consumer could not process (e.g. malformed JSON) so they can be investigated without blocking the partition |
| Poison message | A record that cannot be successfully processed; in V2, routed to DLQ rather than crashing the consumer |
| Order | A customer purchase request: customer ID, line items, total |
| Order event | A downstream event emitted after an order reaches a terminal state (e.g. `OrderFulfilled`) |
| Order ID | UUIDv4 assigned by `order-api` when an order is accepted |

---

## 3. Requirements

Requirements use [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) keywords:
**MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, **MAY**.

### 3.1 System Components

| ID | Requirement |
|----|-------------|
| OPS-01 | The system **MUST** consist of three services: `order-api`, `fulfillment-service`, `notification-service`. |
| OPS-02 | The system **MUST** run against a single Kafka broker in KRaft mode and a single Postgres database. |
| OPS-03 | The system **MUST** use three Kafka topics: `orders`, `order-events`, and `orders.dlq`. |
| OPS-04 | Each topic **MUST** have 3 partitions and replication factor 1. |

Topology:

```
client ──POST /orders──▶ order-api ──▶ [orders] ──▶ fulfillment-service ──▶ Postgres
                                                          │                  ├ processed_orders
                                                          │                  └ outbox
                                                          │                       │
                                                          │              outbox-relay (in-process thread)
                                                          │                       │
                                                          │                       ▼
                                                          │                  [order-events] ──▶ notification-service
                                                          │
                                                          └─ on parse failure ──▶ [orders.dlq]
```

### 3.2 Order API (HTTP Producer)

| ID | Requirement |
|----|-------------|
| OPS-10 | `order-api` **MUST** expose `POST /orders` accepting JSON. |
| OPS-11 | When no `Idempotency-Key` header is supplied, `order-api` **MUST** generate a UUIDv4 `orderId` server-side. |
| OPS-12 | `order-api` **MUST** publish exactly one Kafka record to `orders` with `key = orderId`, `value = Order JSON`. |
| OPS-13 | `order-api` **MUST** respond `202 Accepted` with body `{"orderId": "<uuid>"}` once the producer has received an ack from the broker. |
| OPS-14 | `order-api` **MUST** use `acks=all` and `enable.idempotence=true`. *(V1 used `acks=1`; V2 hardens for the producer-side retry path.)* |
| OPS-15 | `order-api` **MUST** expose `GET /health` → `{"status": "ok"}`. |
| OPS-16 | `order-api` **MUST** reject malformed request bodies with `400 Bad Request` and a JSON error body. |
| OPS-17 | `order-api` **MAY** accept an `Idempotency-Key` HTTP header. When present and previously seen within the last 24 h, `order-api` **MUST** return the same `orderId` it issued for that key without producing a new Kafka record. When present and unseen, the key-to-orderId mapping **MUST** be persisted before the response. *(See §3.9 for storage.)* |

### 3.3 Order Message Format

| ID | Requirement |
|----|-------------|
| OPS-20 | `Order` JSON **MUST** have fields: `orderId`, `customerId`, `items`, `total`, `placedAt`. |
| OPS-21 | `items` **MUST** be a non-empty array of `{sku: string, quantity: int, unitPrice: decimal}`. |
| OPS-22 | `total` **MUST** be computed server-side and **MUST NOT** be trusted from the request. |
| OPS-23 | `placedAt` **MUST** be server-assigned ISO-8601 UTC. |

### 3.4 Fulfillment Consumer

| ID | Requirement |
|----|-------------|
| OPS-30 | `fulfillment-service` **MUST** subscribe to `orders` with `group.id=fulfillment`. |
| OPS-31 | `fulfillment-service` **MUST** use `enable.auto.commit=false` and **MUST** call `commitSync` only after the DB transaction in OPS-33 has committed. *(Replaces V1 OPS-31 auto-commit.)* |
| OPS-32 | `fulfillment-service` **MUST** deserialize records as `byte[]` and parse JSON manually. On parse failure see §3.11. |
| OPS-33 | For a successfully-parsed `Order`, `fulfillment-service` **MUST** check `processed_orders` by `orderId`. If a row exists, the record **MUST** be skipped (idempotency, fixes F1/F2). If no row exists, the service **MUST** within a single Postgres transaction insert one row into `processed_orders` and one row into `outbox`. |
| OPS-34 | The `outbox` row **MUST** carry the topic, key, and serialized payload of the `OrderFulfilled` event to be published. The Kafka publish itself **MUST NOT** happen inside this transaction. *(Decoupling DB durability from Kafka availability is the point of the outbox pattern.)* |
| OPS-35 | `fulfillment-service` **MUST** simulate 50 ms of work between parse and DB transaction. *(Inherited V1 behavior; supports F5 baseline regression checks.)* |

### 3.5 OrderFulfilled Message Format

| ID | Requirement |
|----|-------------|
| OPS-40 | `OrderFulfilled` JSON **MUST** have fields: `orderId`, `customerId`, `fulfilledAt`. |
| OPS-41 | `fulfilledAt` **MUST** be ISO-8601 UTC assigned at fulfillment time. |

### 3.6 Notification Consumer

| ID | Requirement |
|----|-------------|
| OPS-50 | `notification-service` **MUST** subscribe to `order-events` with `group.id=notifications`. |
| OPS-51 | `notification-service` **MUST** use `enable.auto.commit=false` and **MUST** call `commitSync` after each successful notification log emit. *(Replaces V1 OPS-51 auto-commit.)* |
| OPS-52 | On receipt of `OrderFulfilled`, `notification-service` **MUST** log `Notification sent for order=<id> customer=<id>` at `INFO`. No external calls. |
| OPS-53 | `notification-service` **MUST NOT** maintain its own idempotency table. Upstream idempotency in `fulfillment-service` (§3.9) is the source of truth; notification-side dedup is v2.3.0 hardening. *(Known v2.0.0 limitation: a relay-crash-mid-publish window can cause one duplicate notification per crash. See §5 and §7 Q14.)* |

### 3.7 Kafka Broker Configuration

| ID | Requirement |
|----|-------------|
| OPS-60 | The broker **MUST** run in KRaft mode. |
| OPS-61 | `auto.create.topics.enable` **MUST** be `false`; topics **MUST** be created explicitly at startup via `AdminClient`. *(Replaces V1 OPS-61; V2 owns its topic configuration.)* |
| OPS-62 | The broker **MUST** advertise dual listeners: `PLAINTEXT://localhost:9092` for host access and `INTERNAL://kafka:9093` for docker-network access. |

### 3.8 Operational

| ID | Requirement |
|----|-------------|
| OPS-70 | `docker-compose up` **MUST** start Kafka, Postgres, and all three services. |
| OPS-71 | `fulfillment-service` **MUST** run database migrations (Flyway, see §3.10) on startup before the consumer subscribes. |
| OPS-72 | Each service **MUST** handle `SIGTERM` with graceful shutdown: drain the outbox relay one final time, flush in-flight producer batches, `commitSync` the consumer's current offsets, close the consumer, exit within 10 s. |
| OPS-73 | Services **MUST** emit structured JSON logs; `orderId` **MUST** be present in MDC for any log line scoped to a specific order. |
| OPS-74 | A root `Makefile` **MUST** expose targets: `build`, `test`, `chaos`, `run` (everything in docker), `run-local` (Kafka + Postgres in docker, services on host). |

### 3.9 Idempotency

| ID | Requirement |
|----|-------------|
| OPS-100 | A Postgres table `processed_orders` **MUST** exist with columns: `order_id` (UUID, primary key), `customer_id` (text), `fulfilled_at` (timestamptz), `created_at` (timestamptz, default `now()`). |
| OPS-101 | Before any fulfillment work, `fulfillment-service` **MUST** look up `processed_orders` by `order_id`. If a row exists, the service **MUST NOT** re-execute fulfillment, **MUST NOT** insert a new outbox row, and **MUST** commit the Kafka offset. *(Single mechanism that fixes both F1 producer-retry duplicates and F2 redelivery duplicates.)* |
| OPS-102 | A Postgres table `idempotency_keys` **MUST** exist with columns: `key` (text, primary key), `order_id` (UUID), `created_at` (timestamptz). |
| OPS-103 | When `order-api` receives a request with an `Idempotency-Key` header, it **MUST** check `idempotency_keys` by `key` before producing. On hit, **MUST** return the stored `order_id`. On miss, **MUST** insert `(key, generated_orderId)` and produce as normal. |
| OPS-104 | Rows in `idempotency_keys` older than 24 h **MAY** be reaped by a background task. Reaping is not required for correctness; the table is treated as eventually-truncated. |

### 3.10 Transactional Outbox

| ID | Requirement |
|----|-------------|
| OPS-110 | A Postgres table `outbox` **MUST** exist with columns: `id` (bigserial, primary key), `aggregate_id` (text — the `orderId`), `topic` (text), `record_key` (text), `payload` (bytea), `created_at` (timestamptz, default `now()`), `published_at` (timestamptz, nullable). |
| OPS-111 | `fulfillment-service` **MUST** insert the `processed_orders` row and the `outbox` row in a single Postgres transaction. The Kafka publish **MUST NOT** be part of this transaction. |
| OPS-112 | A separate **outbox relay** thread inside `fulfillment-service` **MUST** poll `outbox WHERE published_at IS NULL ORDER BY id LIMIT 100` at a configurable interval (default 100 ms), publish each record to its `topic` via the Kafka producer, await broker ack, and `UPDATE outbox SET published_at = now() WHERE id = ?`. |
| OPS-113 | The outbox relay **MUST** use an idempotent producer (`enable.idempotence=true`, `acks=all`). Publish failures **MUST** be retried in the next poll cycle; the row stays unpublished until ack. |
| OPS-114 | Database schema migrations **MUST** be managed by Flyway with migration files under `fulfillment-service/src/main/resources/db/migration/`. |
| OPS-115 | The outbox relay **MUST** run in the same JVM process as the fulfillment consumer. *(Multi-instance fulfillment with `SELECT FOR UPDATE SKIP LOCKED` on outbox is v2.2.0 work.)* |
| OPS-116 | The outbox relay's Kafka publish loop **MUST** be at-least-once semantics. Duplicate publishes are absorbed by `processed_orders` for upstream dedup; duplicate `order-events` records observed by `notification-service` are a known V2 limitation (see §3.6 OPS-53). |

### 3.11 Dead Letter Queue

| ID | Requirement |
|----|-------------|
| OPS-120 | A topic `orders.dlq` **MUST** exist, 3 partitions, RF 1. |
| OPS-121 | When `fulfillment-service` cannot deserialize a record (malformed JSON, schema mismatch), it **MUST** publish the original raw bytes to `orders.dlq` and **MUST** commit the source offset. |
| OPS-122 | DLQ records **MUST** carry these Kafka headers, in addition to the original key/value bytes: `x-dlq-reason` (the exception class + message), `x-dlq-source-topic`, `x-dlq-source-partition`, `x-dlq-source-offset`, `x-dlq-original-timestamp`. |
| OPS-123 | v2.0.0 routes a record to DLQ on the **first** parse failure. Retry-before-DLQ (a separate retry topic with bounded attempts) is v2.3.0 hardening. |
| OPS-124 | DLQ records **MUST** be consumable from outside the system using `kafka-console-consumer` for manual triage. The system itself **MUST NOT** automatically reprocess DLQ records in V2. |

---

## 4. Inputs / Outputs

### HTTP contract

```
POST /orders
  Content-Type: application/json
  Idempotency-Key: <opaque string>      (optional)
  {
    "customerId": "cust-123",
    "items": [
      { "sku": "SKU-A", "quantity": 2, "unitPrice": 9.99 },
      { "sku": "SKU-B", "quantity": 1, "unitPrice": 4.50 }
    ]
  }

  202 Accepted
  Content-Type: application/json
  { "orderId": "5f1c…-uuid" }

  400 Bad Request
  { "error": "items array is required and non-empty" }


GET /health
  200 OK
  { "status": "ok" }
```

When the same `Idempotency-Key` is replayed within 24 h, the response body
contains the originally-issued `orderId` and no Kafka record is produced.

### Kafka topic: `orders`

```
key:   "<orderId>"
value: <Order JSON, see V1 §4>
```

### Kafka topic: `order-events`

```
key:   "<orderId>"
value: <OrderFulfilled JSON, see V1 §4>
```

### Kafka topic: `orders.dlq`

```
key:   <original key bytes — may not be a valid string>
value: <original value bytes — may not be valid JSON>
headers:
  x-dlq-reason             : "com.fasterxml.jackson.core.JsonParseException: …"
  x-dlq-source-topic       : "orders"
  x-dlq-source-partition   : "1"
  x-dlq-source-offset      : "42"
  x-dlq-original-timestamp : "1714325000000"
```

### Postgres schema

```
CREATE TABLE processed_orders (
  order_id      UUID         PRIMARY KEY,
  customer_id   TEXT         NOT NULL,
  fulfilled_at  TIMESTAMPTZ  NOT NULL,
  created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE outbox (
  id             BIGSERIAL    PRIMARY KEY,
  aggregate_id   TEXT         NOT NULL,
  topic          TEXT         NOT NULL,
  record_key     TEXT         NOT NULL,
  payload        BYTEA        NOT NULL,
  created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
  published_at   TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE idempotency_keys (
  key         TEXT         PRIMARY KEY,
  order_id    UUID         NOT NULL,
  created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
```

### CLI flags

```
order-api
  --port                int     HTTP port (default 6080)
  --bootstrap-servers   string  Kafka bootstrap (default localhost:9092)
  --jdbc-url            string  Postgres JDBC URL (default jdbc:postgresql://localhost:5432/orders)
  --jdbc-user           string  Postgres user (default orders)
  --jdbc-password       string  Postgres password (default orders)

fulfillment-service
  --bootstrap-servers   string
  --jdbc-url            string
  --jdbc-user           string
  --jdbc-password       string
  --outbox-poll-ms      int     Outbox relay poll interval (default 100)

notification-service
  --bootstrap-servers   string
```

---

## 5. Out of Scope

Deferred to v2.1.0 / v2.2.0 / v2.3.0+ as marked. Anything that fundamentally
re-architects the system (Kafka transactions in place of outbox, sagas across
multiple aggregates, etc.) is V3 territory.

- **F5 — consumer lag under burst load** *(v2.1.0)*. v2.0.0 is correctness-focused;
  throughput tuning (intra-instance parallelism via virtual threads,
  `max.poll.records` sizing, async outbox publish) is v2.1.0. The F5 chaos
  test continues to assert the v2.0.0 lag baseline.
- **Multi-instance fulfillment** *(v2.2.0)*. Multiple fulfillment-service
  replicas need `SELECT FOR UPDATE SKIP LOCKED` on the outbox poll to avoid
  duplicate publishes; partition count likely bumps from 3 → 6+.
- **Notification-side idempotency** *(v2.3.0)*. A relay-crash-mid-publish
  window can produce one duplicate notification. A `processed_notifications`
  table in `notification-service` closes the window.
- **Retry-before-DLQ** *(v2.3.0)*. A separate retry topic with bounded attempts
  before final DLQ routing.
- **Prometheus metrics** *(v2.x or Build 5)*.
- Saga pattern with compensating actions (payment → inventory → ship) — V3.
- Distributed tracing (Build 5).
- Schema registry, Avro/Protobuf (plain JSON only).
- Authentication, TLS, ACLs.

---

## 6. V1 Failure Modes Resolved in V2

V1 §6 listed five deliberate failure modes (F1–F5). V2 fixes four of them.
Each row maps the V1 bug to its V2 fix mechanism and the requirements that
implement it.

| # | Failure | V1 behaviour | V2 fix mechanism | Requirements |
|---|---------|-------------|-----------------|--------------|
| F1 | Duplicate orders from client retries | 2 records on `orders` → 2 fulfillments → 2 events | `processed_orders` idempotency check; HTTP `Idempotency-Key` header for an additional layer | OPS-101, OPS-103 |
| F2 | Duplicate notifications after consumer crash | Auto-commit hadn't fired → record redelivered → second event | `enable.auto.commit=false` + `commitSync` after DB transaction; idempotency check absorbs redelivery | OPS-31, OPS-101 |
| F3 | Lost orders after premature commit | Auto-commit advanced past unprocessed record | `commitSync` only after DB transaction commits; durable state is the gate, not the timer | OPS-31, OPS-111 |
| F4 | Poison message stalls partition | Deserialization exception killed consumer thread | Deserialize as `byte[]`, parse manually; route parse failures to `orders.dlq` and continue | OPS-32, OPS-121, OPS-122 |
| F5 | Consumer lag under burst load | 50 ms work × single thread → linear lag growth | **Not addressed in v2.0.0.** Throughput work deferred to v2.1.0. | — |

The chaos test suite at `chaos-test/` retains its V1-baseline assertions for
all five tests. V2 implementation work flips F1–F4 assertions to require the
fix; F5 assertion stays as-is and is the v2.1.0 entry point.

| # | Test class | V1 assertion | V2 assertion |
|---|-----------|-------------|--------------|
| F1 | `F1_DuplicateOrdersTest` | `assertEquals(2, events.size())` | `assertEquals(1, events.size())` |
| F2 | `F2_DuplicateNotificationsTest` | `assertEquals(2, events.size())` | `assertEquals(1, events.size())` |
| F3 | `F3_LostOrdersTest` | `assertEquals(0, events.size())` | `assertEquals(1, events.size())` |
| F4 | `F4_PoisonMessageTest` | `assertTrue(consumer.crashed())`, `assertEquals(0, validEvents)` | `assertFalse(consumer.crashed())`, `assertEquals(1, validEvents)`, `assertEquals(1, dlqRecords)` |
| F5 | `F5_ConsumerLagTest` | `assertTrue(lag > 200)` | unchanged in v2.0.0 — v2.1.0 will invert |

---

## 7. Resolved Decisions

V1 entries Q1–Q7 are unchanged; V2 adds Q8–Q14.

| # | Question | Decision |
|---|----------|----------|
| Q1 | Maven or Gradle? | **Maven** multi-module. |
| Q2 | Spring Kafka or plain `kafka-clients`? | **Plain `kafka-clients`.** |
| Q3 | Kafka image? | **Apache Kafka** (`apache/kafka:3.8.1`) in KRaft mode. |
| Q4 | Fulfillment persistence? | *(V1: in-memory)*. **V2: Postgres** for `processed_orders`, `outbox`, `idempotency_keys`. |
| Q5 | Schema format? | **Plain JSON.** |
| Q6 | Partition count? | **3.** |
| Q7 | `order-api` blocking ack vs fire-and-forget? | **Block on `producer.send(...).get()`.** |
| Q8 | Database engine for idempotency + outbox? | **Postgres 16** in docker-compose. Production-realistic; matches outbox-pattern literature; portfolio signal stronger than embedded H2. |
| Q9 | Kafka native transactions or DB-backed outbox? | **DB-backed outbox.** Decouples DB durability from Kafka availability; avoids transactional-producer complexity and the DB↔Kafka coordination problem. Consequence: notification-side dedup is v2.3.0, not v2.0.0. |
| Q10 | Outbox relay deployment? | **In-process inside `fulfillment-service`** as a separate thread sharing the connection pool. Multi-instance fulfillment with outbox-row locking is v2.2.0 architectural work. |
| Q11 | Retry-before-DLQ? | **Straight to DLQ on first parse failure.** Retry topics with bounded attempts are v2.3.0 hardening. |
| Q12 | `Idempotency-Key` header on `POST /orders`? | **Optional**, with 24 h TTL on the key→orderId mapping. Matches Stripe/Shopify conventions and provides HTTP-layer dedup before any record reaches Kafka. |
| Q13 | DB migration tool? | **Flyway**, migrations under `fulfillment-service/src/main/resources/db/migration/`. Idiomatic Java, zero runtime config. |
| Q14 | F5 (consumer lag) in v2.0.0 scope? | **Deferred to v2.1.0.** v2.0.0 is correctness; F5 is throughput. F5 chaos test stays asserting the V1 lag baseline; v2.1.0 inverts it. |

---

## 8. Open Questions

| # | Question | Owner | Due |
|---|----------|-------|-----|
| *(none outstanding for V2)* | | | |

---

## 9. Revision History

| Version | Date | Author | Notes |
|---------|------|--------|-------|
| 0.1 | 2026-04-24 | Steve Weiland | Initial V1 draft — three services, auto-commit, no idempotency, no DLQ |
| 0.2 | 2026-04-28 | Steve Weiland | v2.0.0: Postgres + idempotency table (F1, F2 fix), transactional outbox + manual commit (F2, F3 fix), DLQ for poison messages (F4 fix). F5 deferred to v2.1.0. |
