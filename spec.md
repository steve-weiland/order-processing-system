# Event-Driven Order Processing System

| Field   | Value              |
|---------|--------------------|
| Version | 0.9                |
| Author  | Steve Weiland      |
| Date    | 2026-09-04         |
| Status  | Current            |

---

## 1. Overview

This spec is layered: each version after V1 added a section. As of v0.6 the
system spans **V1 (deliberately broken baseline) → v2.0.0 (correctness) →
v2.1.0 (throughput) → v2.2.0 (multi-instance) → v2.3.0 (notification dedup)
→ V3.0.0 (saga pattern) → v3.1.0 (review hardening)**. The roadmap and shipped/open status are tracked
in [`README.md`](./README.md). The text below preserves the version that
introduced each requirement so the spec can be read as the V1 → V3 evolution
in commit order.

v2.0.0 fixed the four correctness failures documented in V1 (F1–F4) using
three mechanisms:

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

v2.1.0 adds a fourth mechanism on top of the v2.0.0 correctness work:

4. **Parallel batch processing** — the fulfillment consumer dispatches each
   `poll()` batch onto a virtual-thread executor (Java 21). Records are
   processed concurrently, bounded by a configurable semaphore. Offsets are
   committed only after the entire batch completes; a partial-batch failure
   leaves all offsets in the batch uncommitted, relying on at-least-once
   redelivery + `processed_orders` idempotency to converge. The outbox
   relay also moves to async batch publish: all `producer.send` calls issue
   before any await; `markPublished` becomes a single batched UPDATE.

This is fundamentally enabled by a **domain insight**: records with
different `orderId` are independent, so the poll batch is processed in
parallel across keys. Records with the SAME `orderId` are duplicates — but
"absorbed by the idempotency check" turned out to be a V2.x-only truth:
once V3 gave the saga side-effecting steps that run *before* the
idempotency claim, concurrent duplicates could double-execute them (F13).
v3.1.0 therefore serializes *within* a key (batch dedupe + per-order
advisory lock, §3.13 OPS-205) while staying fully parallel *across* keys.
See Q32.

v3.1.0 (review hardening) closed five defect classes found in a code
review of v3.0.0, each pinned by a new chaos test: **F12** (compensation
was not resumable — a saga redelivered mid-compensation fell through to a
bogus `COMPLETED`), **F13** (concurrent duplicate deliveries
double-executed saga steps), **F14** (a produce failure permanently
poisoned an `Idempotency-Key`), **F15** (notification-service still had
the F4 poison vulnerability), plus the failed-saga row shape (OPS-100)
and a broken `make run-local` (OPS-74).

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
| Auto-commit | Consumer config that commits offsets on a background timer regardless of processing state. **Disabled in v2.0.0.** |
| Manual commit | Application calls `commitSync` after work succeeds. **v2.0.0 default.** |
| Per-batch commit | `commitSync` runs once per `poll()` batch (not per record), only after every record in the batch has succeeded. **v2.1.0 default.** |
| Order-independent processing | Records with different `orderId` are independent — fully parallel across keys. Records with the same `orderId` are duplicates and are **serialized** since v3.1.0 (batch dedupe + per-order lock); the idempotency check alone stopped being a sufficient absorber once saga steps grew side effects. |
| Per-order lock | Postgres *session* advisory lock keyed on the orderId (`pg_advisory_lock(hashtextextended(orderId, 0))`), held for a saga run's full duration on the single connection that also carries its state transitions. *(v3.1.0)* |
| Virtual thread | Java 21 lightweight thread (Project Loom). Cheap to create, doesn't pin an OS thread on blocking operations. v2.1.0 uses one virtual thread per record in a poll batch. |
| Bounded concurrency | A semaphore caps the number of concurrent in-flight records. Defaults to `--worker-pool-size` (32). |
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
| Saga | A sequence of local transactions where each transaction has a defined compensating transaction. *(V3.0.0)* |
| Orchestrated saga | A single coordinator drives the saga's steps in order, in contrast to *choreography* where each step subscribes to events from the previous one. V3.0.0 uses orchestration. |
| Saga step | One local transaction with `execute()` and `compensate()` methods. V3.0.0 has three: `payment`, `inventory`, `shipping`. |
| Compensating action | An operation that semantically undoes a completed step (refund payment, release reserved stock, cancel shipment). |

---

## 3. Requirements

Requirements use [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) keywords:
**MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, **MAY**.

### 3.1 System Components

| ID | Requirement |
|----|-------------|
| OPS-01 | The system **MUST** consist of three services: `order-api`, `fulfillment-service`, `notification-service`. |
| OPS-02 | The system **MUST** run against a single Kafka broker in KRaft mode and a single Postgres database. |
| OPS-03 | The system **MUST** use four Kafka topics: `orders`, `order-events`, `orders.dlq`, and `order-events.dlq`. *(v3.1.0 adds `order-events.dlq`.)* |
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

              notification-service ── on parse failure ──▶ [order-events.dlq]
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
| OPS-17 | `order-api` **MAY** accept an `Idempotency-Key` HTTP header. A key is claimed before the produce and **confirmed** only after the broker ack (§3.9 OPS-103). When a **confirmed** key is replayed within 24 h, `order-api` **MUST** return the same `orderId` it issued for that key without producing a new Kafka record. When a **pending** key is replayed (a previous attempt's produce never succeeded), `order-api` **MUST** re-produce with the stored `orderId` and then confirm. *(v3.1.0; the original claim-then-produce flow poisoned a key on produce failure — F14.)* |

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
| OPS-31 | `fulfillment-service` **MUST** use `enable.auto.commit=false`. *(Replaces V1 OPS-31 auto-commit.)* |
| OPS-32 | `fulfillment-service` **MUST** deserialize records as `byte[]` and parse JSON manually. On parse failure see §3.11. |
| OPS-33 | For a successfully-parsed `Order`, `fulfillment-service` **MUST** invoke the saga orchestrator (§3.13) and wait for it to terminate. On `COMPLETED`, the service **MUST** within a single Postgres transaction insert one row into `processed_orders` and one row into `outbox`. On `FAILED`, the service **MUST** insert one row into `processed_orders` with `status='FAILED'` and NULL `fulfilled_at` (idempotency; §3.9 OPS-100) and **MUST NOT** insert an outbox row. *(V3.0.0; replaces the V2.x `Thread.sleep(50) + INSERT` shape. v3.1.0 adds the row shape.)* |
| OPS-34 | The `outbox` row **MUST** carry the topic, key, and serialized payload of the `OrderFulfilled` event to be published. The Kafka publish itself **MUST NOT** happen inside this transaction. *(Decoupling DB durability from Kafka availability is the point of the outbox pattern.)* |
| OPS-36 | `fulfillment-service` **MUST** dispatch every record in a `poll()` batch onto a virtual-thread executor and process them concurrently. *(v2.1.0; fixes F5.)* |
| OPS-37 | `fulfillment-service` **MUST** call `commitSync` exactly once per batch, only after every record in the batch has completed successfully. If any record fails, no offset in the batch **MUST** be committed; the consumer **MUST** seek every partition of the failed batch back to the batch's first offset (paced by a short pause) so the very next poll replays it — merely skipping the commit lets a later batch commit past the failed record and lose it (§6 F16). The `processed_orders` idempotency check absorbs records that already wrote durable state. |
| OPS-38 | The number of concurrent in-flight records **MUST** be capped by a semaphore configured via `--worker-pool-size` (default 32). The connection pool **MUST** be sized past the semaphore (`workerPoolSize + headroom`) — since v3.1.0 every in-flight saga holds one connection for its full run (OPS-205), so a smaller pool becomes the real concurrency bound (F5 regresses). |
| OPS-39 | Before dispatch, `fulfillment-service` **MUST** drop exact duplicates within a poll batch — records identical in (partition, key, value bytes) — keeping the last occurrence so its committed offset covers the dropped ones. Same-key records with **different** payloads **MUST NOT** be deduped (they are distinct records — e.g. F4's poison + valid pair sharing a partitioning key). *(v3.1.0; first line of defense for F13.)* |

### 3.5 OrderFulfilled Message Format

| ID | Requirement |
|----|-------------|
| OPS-40 | `OrderFulfilled` JSON **MUST** have fields: `orderId`, `customerId`, `fulfilledAt`. |
| OPS-41 | `fulfilledAt` **MUST** be ISO-8601 UTC assigned at fulfillment time. |

### 3.6 Notification Consumer

| ID | Requirement |
|----|-------------|
| OPS-50 | `notification-service` **MUST** subscribe to `order-events` with `group.id=notifications`. |
| OPS-51 | `notification-service` **MUST** use `enable.auto.commit=false` and **MUST** call `commitSync` after each successful notification (logged or skipped as duplicate). *(Replaces V1 OPS-51 auto-commit.)* |
| OPS-52 | On receipt of `OrderFulfilled`, `notification-service` **MUST** atomically claim the `orderId` in `processed_notifications` (see §3.12). On successful claim, **MUST** log `Notification sent for order=<id> customer=<id>` at `INFO`. On conflict (`orderId` already present), **MUST** skip logging and commit the offset. |
| OPS-53 | `notification-service` **MUST** maintain its own idempotency table (`processed_notifications`) — the source of truth for "this customer has been notified". *(v2.3.0; closes the relay-crash duplicate-publish window left open by v2.2.0.)* |
| OPS-54 | `notification-service` **MUST** deserialize records as `byte[]` and parse JSON manually. On parse failure it **MUST** publish the raw record to `order-events.dlq` with the OPS-122 diagnostic headers, commit the source offset, and continue. *(v3.1.0; closes F15 — the F4 lesson applied to this consumer.)* |

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
| OPS-71 | `fulfillment-service` **MUST** run database migrations (Flyway, see §3.10) on startup before the consumer subscribes. Concurrent migrations across multiple instances are safe — Flyway holds a Postgres advisory lock on its history table. |
| OPS-72 | Each service **MUST** handle `SIGTERM` with graceful shutdown: drain the outbox relay one final time, flush in-flight producer batches, `commitSync` the consumer's current offsets, close the consumer, exit within 10 s. |
| OPS-73 | Services **MUST** emit structured JSON logs; `orderId` **MUST** be present in MDC for any log line scoped to a specific order. |
| OPS-74 | A root `Makefile` **MUST** expose targets: `build`, `test`, `chaos`, `run` (everything in docker), `run-local` (Kafka + Postgres in docker, services on host). |
| OPS-76 | `docker-compose.yml` **MUST** run **two** `fulfillment-service` instances (`ops-fulfillment-1`, `ops-fulfillment-2`) sharing the broker and Postgres, demonstrating the multi-instance outbox coordination introduced in v2.2.0. |
| OPS-77 | Postgres **MUST** persist its data in a named volume (`pgdata`): orders, sagas, and idempotency keys survive `make stop` / `make run` cycles; `make clean` (`down -v`) is the explicit wipe. The four app services carry `restart: unless-stopped` — they crash fast on terminal consumer errors, and the policy is what makes that design honest. |

### 3.9 Idempotency

| ID | Requirement |
|----|-------------|
| OPS-100 | A Postgres table `processed_orders` **MUST** exist with columns: `order_id` (UUID, primary key), `customer_id` (text), `status` (text: `FULFILLED` or `FAILED`), `fulfilled_at` (timestamptz, nullable — set exactly when `status='FULFILLED'`), `created_at` (timestamptz, default `now()`). *(v3.1.0 adds `status`; previously a FAILED saga was stamped `fulfilled_at=now()` and queries could not tell the outcomes apart.)* |
| OPS-101 | Before any fulfillment work, `fulfillment-service` **MUST** look up `processed_orders` by `order_id`. If a row exists, the service **MUST NOT** re-execute fulfillment, **MUST NOT** insert a new outbox row, and **MUST** commit the Kafka offset. *(Single mechanism that fixes both F1 producer-retry duplicates and F2 redelivery duplicates.)* |
| OPS-102 | A Postgres table `idempotency_keys` **MUST** exist with columns: `key` (text, primary key), `order_id` (UUID), `created_at` (timestamptz), `confirmed_at` (timestamptz, nullable — NULL means the key's produce has not been acked). *(v3.1.0 adds `confirmed_at`.)* |
| OPS-103 | The key lifecycle is two-phase. On a request with an `Idempotency-Key`, `order-api` **MUST** atomically claim the key (`INSERT … ON CONFLICT DO NOTHING`, `confirmed_at` NULL). On a fresh claim or a pending replay it **MUST** produce with the claimed/stored `orderId` and, only after the broker ack, confirm the key. On a confirmed replay it **MUST** return the stored `order_id` without producing. Re-producing on a pending replay is safe: the duplicate is absorbed downstream (OPS-39, OPS-205, OPS-101). *(v3.1.0; the original check-then-insert-then-produce flow poisoned a key when the produce failed — F14.)* |
| OPS-104 | Rows in `idempotency_keys` older than 24 h **MAY** be reaped by a background task. Reaping is not required for correctness; the table is treated as eventually-truncated. |

### 3.10 Transactional Outbox

| ID | Requirement |
|----|-------------|
| OPS-110 | A Postgres table `outbox` **MUST** exist with columns: `id` (bigserial, primary key), `aggregate_id` (text — the `orderId`), `topic` (text), `record_key` (text), `payload` (bytea), `created_at` (timestamptz, default `now()`), `published_at` (timestamptz, nullable). |
| OPS-111 | `fulfillment-service` **MUST** insert the `processed_orders` row and the `outbox` row in a single Postgres transaction. The Kafka publish **MUST NOT** be part of this transaction. |
| OPS-112 | A separate **outbox relay** thread inside `fulfillment-service` **MUST** poll `outbox WHERE published_at IS NULL ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED` at a configurable interval (default 100 ms) inside an explicit DB transaction, and republish unpublished rows to Kafka. The `FOR UPDATE SKIP LOCKED` is the multi-instance coordination primitive (see §7 Q20). |
| OPS-117 | The outbox relay **MUST** issue all `producer.send` calls in the polled batch *before* awaiting any individual ack, then await each `Future<RecordMetadata>` in turn. *(v2.1.0; replaces the v2.0.0 send-then-await-one-by-one loop.)* |
| OPS-118 | After all `Future`s in the batch have completed, the relay **MUST** mark the entire batch published in a single `UPDATE outbox SET published_at = now() WHERE id = ANY(?)` statement. *(v2.1.0.)* |
| OPS-119 | The relay's poll → publish → mark-published cycle **MUST** execute as a single Postgres transaction. The `FOR UPDATE` lock acquired by OPS-112 **MUST** be held through every `producer.send().get()` and the `markPublishedBatch` UPDATE; the transaction commits only after Kafka has acked every record in the batch. *(v2.2.0; releasing the lock before the publish reopens the duplicate-publish window.)* |
| OPS-113 | The outbox relay **MUST** use an idempotent producer (`enable.idempotence=true`, `acks=all`). Publish failures **MUST** be retried in the next poll cycle; the row stays unpublished until ack. |
| OPS-114 | Database schema migrations **MUST** be managed by Flyway with migration files under the `common` module (`common/src/main/resources/db/migration/`), applied by every service at startup from the shared classpath (OPS-132). *(Moved out of fulfillment-service at v2.3.0 when notification-service joined Flyway.)* |
| OPS-115 | The outbox relay **MUST** run in the same JVM process as the fulfillment consumer. *(v2.2.0)* The relay **MAY** run concurrently across multiple JVM instances against the same Postgres; row-level locks acquired by `FOR UPDATE SKIP LOCKED` ensure that each unpublished row is published by exactly one instance. |
| OPS-116 | The outbox relay's Kafka publish loop **MUST** be at-least-once semantics. Duplicate publishes are absorbed by `processed_orders` for upstream dedup; duplicate `order-events` records observed by `notification-service` are a known V2 limitation (see §3.6 OPS-53). |

### 3.13 Saga Orchestrator

| ID | Requirement |
|----|-------------|
| OPS-200 | `fulfillment-service` **MUST** run a saga orchestrator for every new order with three steps in fixed order: payment → inventory → shipping. |
| OPS-201 | The orchestrator **MUST** persist saga state transitions to the `sagas` table before invoking each step and after each step's success or failure. |
| OPS-202 | If any step fails, the orchestrator **MUST** invoke `compensate()` on every previously-completed step in reverse order. |
| OPS-203 | The orchestrator **MUST** be resumable from **any** non-terminal state. On startup or redelivery, if a `sagas` row exists for the orderId in a non-terminal state, the orchestrator **MUST** continue from that state without re-executing already-completed steps. This includes the compensation path: a saga redelivered in `COMPENSATING_INVENTORY` / `COMPENSATING_PAYMENT` **MUST** resume the remaining compensations and reach `FAILED` — it **MUST NOT** be treated as complete. *(v3.1.0; F12 — the v3.0.0 dispatcher fell through to `COMPLETED` here.)* |
| OPS-204 | A saga **MUST** be terminal when reaching `COMPLETED` (success path) or `FAILED` (compensation completed, or no compensation needed). Terminal states **MUST** short-circuit on re-entry. |
| OPS-205 | Saga runs for the same orderId **MUST** be mutually exclusive across workers and instances. The orchestrator **MUST** hold a Postgres *session* advisory lock keyed on the orderId for the run's full duration, re-read saga state after acquiring it, and perform every state read/write on the same connection that holds the lock — one connection per in-flight saga (a second connection per worker lets lock-holders exhaust the pool). *(v3.1.0; F13. Session-scoped, not xact-scoped: transitions must commit individually for OPS-203 crash-resume, and a dead JVM's session releases the lock.)* |

### 3.14 Saga State

| ID | Requirement |
|----|-------------|
| OPS-210 | A Postgres table `sagas` **MUST** exist with columns: `saga_id` UUID PK (= `order_id`), `order_id` UUID UNIQUE, `state` text, `payment_done_at`, `inventory_done_at`, `shipping_done_at` timestamptz nullable, `failure_step` text nullable, `failure_reason` text nullable, `created_at`, `updated_at` timestamptz. |
| OPS-211 | Valid `state` values: `PAYMENT_PENDING`, `INVENTORY_PENDING`, `SHIPPING_PENDING`, `COMPLETED`, `COMPENSATING_INVENTORY`, `COMPENSATING_PAYMENT`, `FAILED`. |
| OPS-212 | The orchestrator **MUST** start a saga via an atomic `INSERT … ON CONFLICT DO NOTHING` and resume from the existing row on conflict. The atomic insert is the gate against double-starting a saga under concurrent redelivery or rebalance. |
| OPS-213 | State transitions **MUST** be implemented as compare-and-swap UPDATEs (`UPDATE … SET state = 'TO' WHERE saga_id = ? AND state = 'FROM'`) so two attempts cannot both advance from the same state. The orchestrator **MUST** check every CAS result: under OPS-205's lock a lost CAS is a logic bug and **MUST** fail the invocation loudly rather than let it keep executing steps it no longer owns. *(v3.1.0; v3.0.0 discarded the results — F13.)* |
| OPS-214 | The transition that **enters** a `COMPENSATING_*` state **MUST** persist `failure_step` and `failure_reason` in the same statement, so a compensation resumed after a crash still knows the original failure without the original exception. *(v3.1.0; F12.)* |

### 3.15 Saga Steps

| ID | Requirement |
|----|-------------|
| OPS-220 | Each step **MUST** implement an interface providing `name()`, `execute(Order)` and `compensate(Order)`. |
| OPS-221 | Each step **MUST** simulate 10 ms of work via `Thread.sleep`. *(V3.0.0 in-process simulation; v3.2.0+ replaces this with real external calls.)* |
| OPS-222 | Each step **MUST** read its failure rate from a per-step env var (`PAYMENT_FAILURE_RATE`, `INVENTORY_FAILURE_RATE`, `SHIPPING_FAILURE_RATE`; default 0.0). When the configured rate triggers, `execute()` **MUST** throw `StepFailedException`. |
| OPS-223 | Compensations **MAY** assume their work succeeds in V3.0.0. Compensation failure handling (with retry, alerting, manual intervention) is V3.1.0+ territory. |
| OPS-224 | Steps **MAY** rely on the orchestrator's state-aware re-entry for idempotency: a step whose completion transition was **committed** is never re-executed. A crash in the window between a step's `execute()` returning and its transition committing re-runs that step on redelivery — inherent at-least-once until per-step idempotency keys land. *(v3.2.0, when steps become real external calls, each step **MUST** carry its own idempotency key — typically `orderId + step.name()`.)* |
| OPS-225 | Compensations **MUST** be idempotent: a crash after `compensate()` returns but before its state transition commits re-runs that compensation on resume (F12). *(v3.1.0.)* |

### 3.12 Notification-Side Idempotency

| ID | Requirement |
|----|-------------|
| OPS-130 | A Postgres table `processed_notifications` **MUST** exist with columns: `order_id` (UUID, primary key), `notified_at` (timestamptz, default `now()`). |
| OPS-131 | Before logging a notification, `notification-service` **MUST** issue `INSERT INTO processed_notifications (order_id) VALUES (?::uuid) ON CONFLICT DO NOTHING`. The row count from this statement (`1` = newly notified, `0` = duplicate) is the dedup decision. |
| OPS-132 | `notification-service` **MUST** connect to the same Postgres database as `order-api` and `fulfillment-service`. Schema migrations are managed by Flyway from the `common` module's classpath, identical to the other services. |
| OPS-133 | The relay-crash duplicate window (a producer-session-bounded duplicate that the idempotent producer cannot dedupe across crashes) **MUST** be absorbed by `processed_notifications`. The atomic INSERT is the gate; concurrent or replayed deliveries lose to the first claimant. |
| OPS-134 | Known, accepted window (v3.1.0 documents it): the claim commits before the emit, so a crash between them drops that notification permanently — the redelivery loses the claim and is suppressed. The claim/emit pair is **at-most-once**. Acceptable while the "send" is a log line; a real sender (v3.2.0+) needs a notification-side outbox — see §8. |

### 3.11 Dead Letter Queue

| ID | Requirement |
|----|-------------|
| OPS-120 | A topic `orders.dlq` **MUST** exist, 3 partitions, RF 1. |
| OPS-121 | When `fulfillment-service` cannot deserialize a record (malformed JSON, schema mismatch), it **MUST** publish the original raw bytes to `orders.dlq` and **MUST** commit the source offset. |
| OPS-122 | DLQ records **MUST** carry these Kafka headers, in addition to the original key/value bytes: `x-dlq-reason` (the exception class + message), `x-dlq-source-topic`, `x-dlq-source-partition`, `x-dlq-source-offset`, `x-dlq-original-timestamp`. |
| OPS-123 | A record is routed to DLQ on the **first** parse failure. Retry-before-DLQ (a separate retry topic with bounded attempts) remains deferred (Q26) — revisit at v3.2.0 alongside per-step retries, when a genuinely transient error path exists. |
| OPS-124 | DLQ records **MUST** be consumable from outside the system using `kafka-console-consumer` for manual triage. The system itself **MUST NOT** automatically reprocess DLQ records. |
| OPS-125 | A topic `order-events.dlq` **MUST** exist, 3 partitions, RF 1. *(v3.1.0)* |
| OPS-126 | `notification-service` parse failures **MUST** be routed to `order-events.dlq` with the OPS-122 headers (`x-dlq-source-topic=order-events`), and the source offset committed (OPS-54). *(v3.1.0; F15.)* |

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
contains the originally-issued `orderId`. No Kafka record is produced —
provided the original produce was acked (a **confirmed** key). Replaying a
**pending** key (the previous attempt's produce failed) re-produces with
the same stored `orderId` before responding (OPS-103).

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

### Kafka topic: `order-events.dlq` *(v3.1.0)*

```
key:   <original key bytes>
value: <original value bytes — malformed OrderFulfilled JSON>
headers: same x-dlq-* set, with x-dlq-source-topic = "order-events"
```

### Postgres schema

```
CREATE TABLE processed_orders (
  order_id      UUID         PRIMARY KEY,
  customer_id   TEXT         NOT NULL,
  status        TEXT         NOT NULL DEFAULT 'FULFILLED',  -- FULFILLED | FAILED (v3.1.0)
  fulfilled_at  TIMESTAMPTZ,          -- set exactly when status = 'FULFILLED'
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
  key           TEXT         PRIMARY KEY,
  order_id      UUID         NOT NULL,
  created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
  confirmed_at  TIMESTAMPTZ           -- NULL = pending: produce not yet acked (v3.1.0)
);

CREATE TABLE processed_notifications (          -- v2.3.0
  order_id    UUID         PRIMARY KEY,
  notified_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE sagas (                            -- V3.0.0
  saga_id            UUID         PRIMARY KEY,  -- = order_id
  order_id           UUID         NOT NULL UNIQUE,
  state              TEXT         NOT NULL,
  payment_done_at    TIMESTAMPTZ,
  inventory_done_at  TIMESTAMPTZ,
  shipping_done_at   TIMESTAMPTZ,
  failure_step       TEXT,        -- written when compensation BEGINS (v3.1.0, OPS-214)
  failure_reason     TEXT,
  created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX sagas_state_idx ON sagas (state)
    WHERE state NOT IN ('COMPLETED', 'FAILED');
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
  --worker-pool-size    int     Max concurrent in-flight records (default 32)

notification-service
  --bootstrap-servers   string
  --jdbc-url            string  (required since v2.3.0 — processed_notifications)
  --jdbc-user           string
  --jdbc-password       string
```

---

## 5. Out of Scope

Deferred to v2.1.0 / v2.2.0 / v2.3.0+ as marked. Anything that fundamentally
re-architects the system (Kafka transactions in place of outbox, sagas across
multiple aggregates, etc.) is V3 territory.

- **Retry-before-DLQ** *(revisit v3.2.0)*. A separate retry topic with bounded
  attempts before final DLQ routing. The original v2.3.0 plan included this,
  but parse failures (the only error path that currently routes to DLQ) are
  structural and deterministic — retrying the same bytes yields the same
  failure. Transient DB errors are already covered by Kafka redelivery + the
  `processed_orders` idempotency check. Deferred until a concrete failure
  mode (e.g., transient downstream HTTP call) makes the case for it.
- **Prometheus metrics** *(v2.x or Build 5)*.
- **Outbox-relay leader election.** An alternative to `SKIP LOCKED` where one instance "owns" outbox publishing. Rejected because `SKIP LOCKED` is stateless and scales horizontally without coordination. Documented here for completeness.
- **Choreography sagas, asynchronous orchestration.** V3.0.0 uses sync orchestration with in-process steps. Each can evolve independently — async orchestrator (request/response over Kafka) → V3.x; full choreography → not planned (orchestration was the deliberate Q27 choice).
- **Separate payment / inventory / shipping services.** V3.0.0 keeps steps in-process to focus on state-machine + compensation correctness. Service split → v3.3.0.
- **Per-step bounded retries.** Any step failure → immediate compensation. Retries pre-compensation become meaningful when steps are real retryable calls; expected v3.2.0.
- **`OrderFailed` event topic.** Failed sagas don't notify customers; the `sagas` table (plus `processed_orders.status`, v3.1.0) is the audit trail. A failure-notification topic is v3.2.0 if a customer-facing failure path lands.
- **Compensation-failure handling.** v3.1.0 made compensation *resumable* (F12) and requires compensations to be *idempotent* (OPS-225); a compensation that itself **fails** (throws) still just redelivers and retries forever. Retry budgets + alerting + manual escalation — v3.2.0.
- Distributed tracing (Build 5).
- Schema registry, Avro/Protobuf (plain JSON only).
- Authentication, TLS, ACLs.

---

## 6. Failure Modes (V1 baseline → V3 final state)

V1 §6 listed five deliberate failure modes (F1–F5). The V2.x line closed
F1–F5 across four minor versions; V2.x exposed two additional failure modes
(F6 in v2.2.0 multi-instance, F7 in the v2.0.0–v2.2.0 single-instance
window) that v2.2.0 / v2.3.0 closed; V3.0.0 introduced the saga pattern,
which surfaced four new saga-specific failure modes (F8–F11) closed in the
same release. v3.1.0 closed four more (F12–F15) found by a code review of
v3.0.0 — three saga-layer bugs the F8–F11 tests were structurally blind to
(they asserted event counts and single-threaded resume, not concurrent
execution or mid-compensation crashes) plus one V1-class bug that had
survived in notification-service. Each row below maps the bug to its fix
mechanism and the requirements that implement it.

| # | Failure | V1 behaviour | V2 fix mechanism | Requirements |
|---|---------|-------------|-----------------|--------------|
| F1 | Duplicate orders from client retries | 2 records on `orders` → 2 fulfillments → 2 events | `processed_orders` idempotency check; HTTP `Idempotency-Key` header for an additional layer | OPS-101, OPS-103 |
| F2 | Duplicate notifications after consumer crash | Auto-commit hadn't fired → record redelivered → second event | `enable.auto.commit=false` + `commitSync` after DB transaction; idempotency check absorbs redelivery | OPS-31, OPS-101 |
| F3 | Lost orders after premature commit | Auto-commit advanced past unprocessed record | `commitSync` only after DB transaction commits; durable state is the gate, not the timer | OPS-31, OPS-111 |
| F4 | Poison message stalls partition | Deserialization exception killed consumer thread | Deserialize as `byte[]`, parse manually; route parse failures to `orders.dlq` and continue | OPS-32, OPS-121, OPS-122 |
| F5 | Consumer lag under burst load | 50 ms work × single thread → linear lag growth | Virtual-thread parallel batch processing in fulfillment-service; bounded concurrency via semaphore; per-batch `commitSync`; async outbox publish | OPS-36, OPS-37, OPS-38, OPS-117, OPS-118 |
| F6 | Multi-instance outbox-poll race | Two relay instances poll the same `WHERE published_at IS NULL` rows | `SELECT FOR UPDATE SKIP LOCKED` inside an explicit DB transaction; lock held across the entire publish + mark-published cycle | OPS-112, OPS-119 |
| F7 | Relay-crash duplicate-publish window | Outbox relay JVM crashes between successful Kafka publish and the `markPublishedBatch` UPDATE; on restart the row is republished and `notification-service` (with no idempotency in v2.0.0–v2.2.0) logs the customer twice | `processed_notifications` table; atomic `INSERT … ON CONFLICT DO NOTHING` gate before logging | OPS-52, OPS-130, OPS-131, OPS-133 |
| F8 | First saga step (payment) fails | No prior steps to compensate; saga transitions directly to `FAILED`; `processed_orders` inserted (idempotency); no outbox row | Saga state machine + `failure_step`/`failure_reason` audit | OPS-200, OPS-201, OPS-211 |
| F9 | Inventory step fails after payment success | Compensate payment in reverse order; saga `FAILED`; no outbox row | OPS-202 reverse-order compensation | OPS-202 |
| F10 | Shipping step fails after payment + inventory success | Compensate inventory then payment in reverse; saga `FAILED`; no outbox row | OPS-202 reverse-order compensation | OPS-202 |
| F11 | Consumer crashes mid-saga (forward path) | On redelivery the orchestrator reads the persisted state and continues from the next pending step; steps whose completion transition was committed are not re-executed | Saga resume-from-state via OPS-203; state-aware re-entry | OPS-203, OPS-213 |
| F12 | Consumer crashes mid-**compensation** | (v3.0.0 bug) The dispatcher handled only `*_PENDING` states; a saga redelivered in `COMPENSATING_*` fell through to a bogus `COMPLETED` — false `OrderFulfilled` event, refund never completed, row stranded | Exhaustive state dispatch; compensation chain resumable from any point; failure cause persisted at compensation entry | OPS-203, OPS-214, OPS-225 |
| F13 | Concurrent duplicate deliveries double-execute steps | (v3.0.0 bug) Two workers both read `PAYMENT_PENDING` and both charged; CAS results were discarded so the loser kept executing too. Invisible to F1, which counts events, not executions | Batch dedupe on (partition, key, value) + per-order session advisory lock + enforced CAS | OPS-39, OPS-205, OPS-213 |
| F14 | Produce failure poisons an `Idempotency-Key` | (v2.0.0 bug) Key committed before the produce; the retry replayed an orderId that never reached Kafka — lost order reported as success | Two-phase claim: pending → confirmed on broker ack; pending replay re-produces with the stored orderId | OPS-17, OPS-102, OPS-103 |
| F15 | Poison message stalls notification-service | (V1-class bug, survived F4) Typed deserializer threw inside `poll()`; consumer died, partition stalled, no DLQ for `order-events` | `byte[]` deserialize + in-loop parse; route to `order-events.dlq`; commit offset, continue | OPS-54, OPS-125, OPS-126 |
| F16 | Transient worker failure silently loses the order | (v2.1.0 bug, found in second review) A failed batch skipped its commit but the poll position kept advancing; the next successful batch on that partition committed offsets past the failed record — never processed, never redelivered. The "redeliver after rebalance" assumption held only if no later commit touched the partition | Failed batch seeks every partition back to its first offset (paced 500 ms) so the re-poll redelivers in the same group generation; batch dedupe + pre-saga idempotency gate + per-order advisory lock absorb the survivors' reprocessing | OPS-37 |

F6 was new in v2.2.0 — it surfaces only with multiple fulfillment-service
instances. F7 was a known v2.0.0–v2.2.0 limitation and is closed in v2.3.0.
F8–F11 are new in V3.0.0 — they only exist once fulfillment becomes a
multi-step state machine. F12–F15 were latent defects found by review and
closed in v3.1.0: F12/F13 were introduced by V3.0.0's saga layer, F14 had
existed since v2.0.0, F15 since v2.3.0 gave notification-service a typed
deserializer.

F6 is **new in v2.2.0** — it didn't exist in V1 (no outbox), or in v2.0.0 / v2.1.0
(single-instance). It surfaces only when fulfillment-service runs in multiple
JVMs against the same Postgres + Kafka.

The chaos test suite at `chaos-test/` retains its V1-baseline assertions
for the five tests that started in V1; v2.0.0 work flipped F1–F4 to assert
the fix; v2.1.0 flipped F5; v2.2.0 added F6; v2.3.0 added F7; V3.0.0 added
F8–F11; v3.1.0 added F12–F15. At HEAD, all 15 failure modes assert their
respective fix.

| # | Test class | V1 assertion | Current assertion |
|---|-----------|-------------|--------------|
| F1 | `F1_DuplicateOrdersTest` | `assertEquals(2, events.size())` | `assertEquals(1, events.size())` |
| F2 | `F2_DuplicateNotificationsTest` | `assertEquals(2, events.size())` | `assertEquals(1, events.size())` |
| F3 | `F3_LostOrdersTest` | `assertEquals(0, events.size())` | `assertEquals(1, events.size())` |
| F4 | `F4_PoisonMessageTest` | `assertTrue(consumer.crashed())`, `assertEquals(0, validEvents)` | `assertFalse(consumer.crashed())`, `assertEquals(1, validEvents)`, `assertEquals(1, dlqRecords)` |
| F5 | `F5_ConsumerLagTest` | `assertTrue(lag > 200)` | flipped in v2.1.0: `assertTrue(lag < 50)` |
| F6 | `F6_MultiInstanceOutboxRaceTest` | (new in v2.2.0) | `assertEquals(N, events.size())` with two concurrent relay instances |
| F7 | `F7_NotificationDedupTest` | (new in v2.3.0) | publish a duplicate `OrderFulfilled` directly to `order-events`; assert exactly one notification logged |
| F8 | `F8_PaymentFailureNoCompensationTest` | (new in V3.0.0) | force payment-step failure; assert saga `FAILED`, no outbox row |
| F9 | `F9_InventoryFailureCompensatesPaymentTest` | (new in V3.0.0) | force inventory-step failure; assert payment compensated, saga `FAILED` |
| F10 | `F10_ShippingFailureCompensatesAllTest` | (new in V3.0.0) | force shipping-step failure; assert inventory + payment compensated in reverse |
| F11 | `F11_SagaResumeAfterCrashTest` | (new in V3.0.0) | invoke orchestrator twice; on second invocation it picks up at the next pending step without re-executing completed ones |
| F12 | `F12_CompensationResumeAfterCrashTest` | (new in v3.1.0) | crash during a compensation; the second run resumes the chain and reaches `FAILED` with the original `failure_step` — the pre-fix code returned `COMPLETED` here |
| F13 | `F13_ConcurrentSagaDoubleExecutionTest` | (new in v3.1.0) | two latched threads run the same order concurrently; every step `executionCount == 1` (pre-fix: payment charged twice); compensation also exactly once |
| F14 | `F14_IdempotencyKeyProduceFailureTest` | (new in v3.1.0) | first produce fails → key left PENDING; retry re-produces the same orderId exactly once and confirms; confirmed replay produces nothing |
| F15 | `F15_NotificationPoisonMessageTest` | (new in v3.1.0) | poison bytes on `order-events` → routed to `order-events.dlq` with headers; the valid event behind it (same partition) still notifies; consumer alive |

---

## 7. Resolved Decisions

V1 entries Q1–Q7; v2.0.0 adds Q8–Q14; v2.1.0 adds Q15–Q19; v2.2.0 adds
Q20–Q23; v2.3.0 adds Q24–Q26; V3.0.0 adds Q27–Q31; v3.1.0 adds Q32–Q35.

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
| Q15 | Virtual threads (Java 21) or Confluent Parallel Consumer library? | **Virtual threads.** ~30 lines of code; `Executors.newVirtualThreadPerTaskExecutor()` is the right primitive; no extra dep; portfolio signal for Java 21 idioms. Confluent Parallel Consumer wins iff per-key ordering becomes a domain requirement (it isn't). |
| Q16 | Fully-parallel within a batch, or per-key serial? | **Fully-parallel.** Idempotency makes ordering irrelevant in this domain (see §2 "Order-independent processing"). Per-key serial would force a `groupBy(orderId)` step for no correctness benefit. |
| Q17 | Bounded or unbounded concurrency? | **Bounded** via semaphore at `--worker-pool-size` (default 32). Virtual threads are cheap, but DB connections aren't (Hikari pool size 16). The semaphore protects against pathological bursts; the connection pool provides the secondary bound. |
| Q18 | Commit cadence? | **Per-batch.** One `commitSync` per `poll()` instead of N. Reduces commit overhead and matches the all-or-nothing batch-failure semantics (OPS-37). |
| Q19 | Outbox relay: per-row send or async batch? | **Async batch.** Issue all `producer.send` calls before awaiting any future, then batch the `markPublished` UPDATE. Order-of-magnitude faster than v2.0.0's serial loop on the same workload. |
| Q20 | How to coordinate multi-instance outbox publishing? | **`SELECT FOR UPDATE SKIP LOCKED`.** Stateless, no leader election, scales linearly. The textbook Postgres work-queue pattern: each row is locked by exactly one instance's transaction; concurrent pollers `SKIP LOCKED` rows and pick up disjoint work. |
| Q21 | Lock duration includes the Kafka publish? | **Yes.** Releasing the lock before the publish reopens the duplicate-publish window — Instance B could lock-and-publish the same row before Instance A's `markPublished` lands. The Kafka round-trip (~5-50 ms per batch) inside the tx is acceptable; the lock holds for the whole batch, not per-record. |
| Q22 | Bump partition count to enable scaling beyond 3 instances? | **No.** 3 partitions is enough to demonstrate the pattern. Higher counts are a deployment / capacity concern, not a correctness one. |
| Q23 | How many fulfillment instances in `docker-compose.yml`? | **Two.** Enough to demonstrate the pattern and to drive the `F6_MultiInstanceOutboxRaceTest` assertion. |
| Q24 | Notification dedup: DB-backed `processed_notifications`, Kafka-header dedup key, or in-memory cache? | **DB-backed.** Matches the `processed_orders` pattern that fulfillment-service already uses. In-memory caches are lost on restart (the consumer would reprocess from `auto.offset.reset=earliest` on a clean group). Kafka-header dedup requires a stateful processor (Kafka Streams or KTable) that's heavier than what V2 needs. |
| Q25 | Should `notification-service` share the same Postgres database as the other services? | **Yes.** Single shared DB is intentional in this system — it avoids cross-database transaction concerns (we don't have any) and keeps Flyway migration ownership simple. A microservices-style "every service owns its database" topology is V3 architectural work. |
| Q26 | Retry-before-DLQ scope for v2.3.0? | **Deferred to v2.4.0+.** Parse failures (the only DLQ trigger today) are deterministic; retrying the same bytes won't change the outcome. Transient errors are already absorbed by Kafka redelivery + idempotency. Will revisit when there's a concrete transient-error path (e.g., an external HTTP call from a future processing step). |
| Q27 | Orchestration or choreography? | **Orchestration.** Single state machine owned by `fulfillment-service` is easier to reason about, debug, and extend than emergent flow from event subscriptions. Choreography is not planned. |
| Q28 | In-process steps or separate services? | **In-process for V3.0.0.** The architectural concern is the saga's state-machine + compensation correctness, not network plumbing. Steps extract to services in V3.1.0+ when the abstraction is stable. |
| Q29 | Synchronous or asynchronous orchestrator? | **Synchronous.** Each step is a method call returning a result. The whole saga lives within one consumer invocation. Async orchestration (request/response over Kafka per step) is V3.x. |
| Q30 | Per-step retries before triggering compensation? | **None in V3.0.0.** Any step failure → compensate. v2.4.0-deferred retry-before-DLQ work becomes meaningful again now that retryable calls exist; layer it on the saga steps in V3.1.0. |
| Q31 | Emit `OrderFailed` events on failed sagas? | **No in V3.0.0.** The `sagas` table is the audit truth; failure observability lives in Postgres. A customer-facing failure-notification topic lands when there's a real failure-recovery path (v3.2.0+). |
| Q32 | (Q16 revisited) Still fully-parallel with no per-key serialization? | **No — serialize within a key, stay parallel across keys.** Q16's premise ("idempotency absorbs same-key duplicates") held while fulfillment was `sleep + INSERT`; V3 gave the saga side-effecting steps that run *before* the idempotency claim, so concurrent duplicates double-executed payment (F13). v3.1.0 adds batch dedupe on (partition, key, value bytes) plus a per-order advisory lock. Cross-key parallelism — the part that fixes F5 — is untouched. |
| Q33 | Per-order mutual exclusion: advisory lock, new `*_EXECUTING` claim states, or Kafka-partition affinity? | **Postgres session advisory lock.** No new states — the 7-state machine and its resume semantics stay intact; works across instances; a crashed JVM's session releases the lock automatically. Cost: one pooled connection held per in-flight saga, so the pool must be sized past the worker semaphore (OPS-38). Xact-scoped locks don't fit: transitions must commit individually for crash-resume, and a commit would drop an xact lock mid-saga. |
| Q34 | `Idempotency-Key` produce failure: delete the key on failure, or pending → confirmed? | **Two-phase (pending → confirmed).** Delete-on-failure still leaks the key if the delete itself fails; a pending replay re-produces with the stored orderId, which downstream dedup makes safe — the same at-least-once-plus-idempotency philosophy as the outbox (Q9). |
| Q35 | DLQ for notification parse failures: reuse `orders.dlq` or a new topic? | **New `order-events.dlq`.** Different triage audience — bad external input vs a bad internally-produced event; the `x-dlq-source-topic` header carries provenance either way. |

---

## 8. Open Questions

| # | Question | Owner | Due |
|---|----------|-------|-----|
| OQ-1 | Notification-side outbox: the claim/emit pair is at-most-once (OPS-134) — fine while the emit is a log line, wrong for a real sender. Claim + payload in one tx with a drain loop, landing with the v3.2.0 sender? | Steve | v3.2.0 |

---

## 9. Revision History

| Version | Date | Author | Notes |
|---------|------|--------|-------|
| 0.1 | 2026-04-24 | Steve Weiland | Initial V1 draft — three services, auto-commit, no idempotency, no DLQ |
| 0.2 | 2026-04-28 | Steve Weiland | v2.0.0: Postgres + idempotency table (F1, F2 fix), transactional outbox + manual commit (F2, F3 fix), DLQ for poison messages (F4 fix). F5 deferred to v2.1.0. |
| 0.3 | 2026-04-28 | Steve Weiland | v2.1.0: virtual-thread parallel batch processing in fulfillment-service (F5 fix); per-batch `commitSync`; bounded concurrency via `--worker-pool-size` semaphore; async outbox relay publish + batched `markPublished`. New OPS-36, OPS-37, OPS-38, OPS-117, OPS-118; resolved Q15-Q19. |
| 0.4 | 2026-04-29 | Steve Weiland | v2.2.0: multi-instance fulfillment via `SELECT FOR UPDATE SKIP LOCKED` on outbox poll, with the lock held across publish + mark-published in a single DB transaction. New F6 failure mode and chaos test; OPS-112 updated, OPS-115 relaxed to allow concurrent relays, OPS-119 added, OPS-76 added (two-instance docker-compose). Resolved Q20-Q23. |
| 0.5 | 2026-04-29 | Steve Weiland | v2.3.0: notification-side idempotency closes the relay-crash duplicate-publish window. New `processed_notifications` table; OPS-52, OPS-53 flipped from "MUST NOT" to "MUST"; new §3.12 with OPS-130–OPS-133. F7 added to §6 / chaos suite. Retry-before-DLQ deferred to v2.4.0+ (Q26). Resolved Q24-Q26. |
| 0.6 | 2026-04-29 | Steve Weiland | V3.0.0: saga pattern (orchestrated, in-process, synchronous) with payment → inventory → shipping. New §3.13 (orchestrator), §3.14 (state), §3.15 (steps); OPS-33 rewritten to delegate to the orchestrator; OPS-35 retired (replaced by per-step `Thread.sleep(10)` in OPS-221). F8–F11 added to §6 / chaos suite. Resolved Q27-Q31. |
| 0.9 | 2026-09-04 | Steve Weiland | Ops hardening: OPS-77 — pgdata named volume (state survives stop/run; clean wipes) and restart: unless-stopped on the app services (crash-fast needs a restarter). |
| 0.8 | 2026-09-04 | Steve Weiland | F16 added to §6 / chaos suite (second review pass): a transient worker failure could silently lose an order — the failed batch skipped its commit but never sought back, so a later batch committed past the record. OPS-37 already required next-poll replay; the code now honors it (seek-back + pace). `F16_TransientFailureLosesOrderTest` red at pre-fix HEAD, green after. |
| 0.7 | 2026-08-26 | Steve Weiland | v3.1.0 review hardening: F12–F15 added to §6 / chaos suite. Resumable compensation (OPS-203/214/225), per-order serialization (OPS-39/205, OPS-213 CAS enforcement), two-phase Idempotency-Key (OPS-17/102/103), notification DLQ (OPS-54/125/126, OPS-03 four topics), failed-saga row shape (OPS-100/33), at-most-once note (OPS-134), pool-sizing rule (OPS-38). Doc reconciliation: OPS-114 migration path, §4 schema/CLI drift, OPS-123↔Q26 contradiction, OPS-224 softened. Resolved Q32–Q35; opened OQ-1. Roadmap shifted (retries → v3.2.0, service split → v3.3.0). |
