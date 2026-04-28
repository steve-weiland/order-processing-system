# Event-Driven Order Processing System — V1

| Field   | Value              |
|---------|--------------------|
| Version | 0.1 (draft)        |
| Author  | Steve Weiland      |
| Date    | 2026-04-24         |
| Status  | Draft              |

---

## 1. Overview

V1 is a three-service event-driven pipeline that accepts customer orders over HTTP,
fulfills them asynchronously via Kafka, and notifies the customer that their order has
been processed.

The system is **deliberately naive**: it uses auto-commit offsets, performs no
idempotency checks, has no dead letter queue, and persists nothing. It works end-to-end
under ideal conditions. It will demonstrably fail under four realistic production
pressures — producer retries (duplicate orders), consumer crashes (duplicate or lost
notifications), poison messages (stuck partitions), and sustained load (consumer lag).

Each V1 failure mode is the documented entry point for a V2 fix: idempotency key table,
transactional outbox, DLQ, and explicit offset management. V1 is built to break in
those exact shapes so the V2 story is concrete, not theoretical.

---

## 2. Definitions

| Term | Definition |
|------|------------|
| Producer | A client that writes records to a Kafka topic |
| Consumer | A client that reads records from a Kafka topic |
| Topic | A named, partitioned, append-only log of records |
| Partition | An ordered subset of a topic; unit of parallelism and ordering |
| Offset | The position of a record within a partition; a 64-bit integer |
| Consumer group | A set of consumers that cooperatively consume a topic; each partition is assigned to exactly one group member |
| Key | A per-record field used for partitioning; records with the same key land on the same partition |
| Auto-commit | Consumer configuration in which offsets are committed periodically by a background thread, regardless of whether the record has been processed |
| Manual commit | Consumer configuration in which the application explicitly calls `commitSync`/`commitAsync` after processing |
| At-most-once | Delivery semantics in which a record may be lost but will never be delivered twice |
| At-least-once | Delivery semantics in which a record may be delivered multiple times but is never lost |
| Exactly-once | Delivery semantics in which a record is delivered exactly once; achievable at the Kafka layer via idempotent producer + transactions, but end-to-end exactly-once requires idempotent consumers too |
| Poison message | A record that cannot be successfully processed and blocks partition progress until handled |
| Consumer lag | The difference between the log-end offset and the consumer's committed offset |
| Order | A customer purchase request: customer ID, line items, total |
| Order event | A downstream event emitted after an order reaches a terminal state (e.g. `OrderFulfilled`) |
| Order ID | UUIDv4 assigned by `order-api` when an order is accepted |
| Idempotency | Property that applying the same operation twice has the same effect as applying it once. **Absent in V1 by design.** |

---

## 3. Requirements

Requirements use [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) keywords:
**MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, **MAY**.

### 3.1 System Components

| ID | Requirement |
|----|-------------|
| OPS-01 | The system **MUST** consist of three services: `order-api`, `fulfillment-service`, `notification-service`. |
| OPS-02 | The system **MUST** run against a single Kafka broker in KRaft mode. |
| OPS-03 | The system **MUST** use two topics: `orders` and `order-events`. |
| OPS-04 | Each topic **MUST** have 3 partitions and replication factor 1. |

Topology:

```
client ──POST /orders──▶ order-api ──▶ [orders] ──▶ fulfillment-service ──▶ [order-events] ──▶ notification-service
                                                           │
                                                           └─ in-memory fulfilled-order map
```

### 3.2 Order API (HTTP Producer)

| ID | Requirement |
|----|-------------|
| OPS-10 | `order-api` **MUST** expose `POST /orders` accepting JSON. |
| OPS-11 | `order-api` **MUST** generate a UUIDv4 `orderId` server-side for every request. |
| OPS-12 | `order-api` **MUST** publish exactly one Kafka record to `orders` with `key = orderId`, `value = Order JSON`. |
| OPS-13 | `order-api` **MUST** respond `202 Accepted` with JSON body `{"orderId": "<uuid>"}` once the producer has received an ack from the broker. |
| OPS-14 | `order-api` **MUST** use `acks=1` and **MUST NOT** enable the idempotent producer (`enable.idempotence=false`). Deliberate V1 limitation, superseded in V2. |
| OPS-15 | `order-api` **MUST** expose `GET /health` → `{"status": "ok"}`. |
| OPS-16 | `order-api` **MUST** reject malformed request bodies with `400 Bad Request` and a JSON error body. Kafka publishing **MUST NOT** be attempted for rejected requests. |

### 3.3 Order Message Format

| ID | Requirement |
|----|-------------|
| OPS-20 | `Order` JSON **MUST** have fields: `orderId`, `customerId`, `items`, `total`, `placedAt`. |
| OPS-21 | `items` **MUST** be a non-empty array of `{sku: string, quantity: int, unitPrice: decimal}`. |
| OPS-22 | `total` **MUST** be computed server-side as `Σ quantity × unitPrice` and **MUST NOT** be trusted from the request body. |
| OPS-23 | `placedAt` **MUST** be server-assigned ISO-8601 UTC at acceptance time. |

### 3.4 Fulfillment Consumer

| ID | Requirement |
|----|-------------|
| OPS-30 | `fulfillment-service` **MUST** subscribe to `orders` with `group.id=fulfillment`. |
| OPS-31 | `fulfillment-service` **MUST** use `enable.auto.commit=true` with default `auto.commit.interval.ms=5000`. Deliberate V1 limitation, superseded in V2. |
| OPS-32 | Fulfillment logic: log the order, simulate 50 ms of work (`Thread.sleep(50)`), insert the order into an in-memory `Map<String, Order>` keyed by `orderId`. |
| OPS-33 | On successful fulfillment, `fulfillment-service` **MUST** produce one `OrderFulfilled` record to `order-events` with `key = orderId`. |
| OPS-34 | `fulfillment-service` **MUST NOT** check whether an `orderId` has already been fulfilled before processing. Deliberate V1 limitation — see §6 F1/F2. |
| OPS-35 | On deserialization failure (poison message), the consumer **MAY** crash. V1 has no DLQ — poison messages are a documented failure mode (§6 F4). |

### 3.5 OrderFulfilled Message Format

| ID | Requirement |
|----|-------------|
| OPS-40 | `OrderFulfilled` JSON **MUST** have fields: `orderId`, `customerId`, `fulfilledAt`. |
| OPS-41 | `fulfilledAt` **MUST** be ISO-8601 UTC assigned at fulfillment time. |

### 3.6 Notification Consumer

| ID | Requirement |
|----|-------------|
| OPS-50 | `notification-service` **MUST** subscribe to `order-events` with `group.id=notifications`. |
| OPS-51 | `notification-service` **MUST** use `enable.auto.commit=true`. |
| OPS-52 | On receipt of `OrderFulfilled`, `notification-service` **MUST** log `Notification sent for order=<id> customer=<id>` at `INFO`. No external calls in V1. |
| OPS-53 | `notification-service` **MUST NOT** deduplicate notifications. Duplicate deliveries are a documented failure mode (§6 F2). |

### 3.7 Kafka Broker Configuration

| ID | Requirement |
|----|-------------|
| OPS-60 | The broker **MUST** run in KRaft mode (no ZooKeeper). |
| OPS-61 | `auto.create.topics.enable` **MUST** be `true`. V1 convenience; V2 will create topics explicitly via admin client. |
| OPS-62 | The broker **MUST** advertise dual listeners: `PLAINTEXT://localhost:9092` for host access and `INTERNAL://kafka:9093` for docker-network access. |

### 3.8 Operational

| ID | Requirement |
|----|-------------|
| OPS-70 | `docker-compose up` **MUST** start the broker plus all three services. |
| OPS-71 | Each service **MUST** handle `SIGTERM` with graceful shutdown: flush in-flight producer batches, close the consumer, exit within 10 s. |
| OPS-72 | Services **MUST** emit structured JSON logs; `orderId` **MUST** be present in MDC for any log line scoped to a specific order. |
| OPS-73 | A root `Makefile` **MUST** expose targets: `build`, `test`, `run` (everything in docker), `run-local` (Kafka in docker, services on host). |

---

## 4. Inputs / Outputs

### HTTP contract

```
POST /orders
  Content-Type: application/json
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
  Content-Type: application/json
  { "error": "items array is required and non-empty" }


GET /health
  200 OK
  { "status": "ok" }
```

### Kafka topic: `orders`

```
key:   "<orderId>"
value: {
  "orderId":    "5f1c…",
  "customerId": "cust-123",
  "items":      [ { "sku": "SKU-A", "quantity": 2, "unitPrice": 9.99 }, … ],
  "total":      24.48,
  "placedAt":   "2026-04-24T14:03:22.481Z"
}
```

### Kafka topic: `order-events`

```
key:   "<orderId>"
value: {
  "orderId":     "5f1c…",
  "customerId":  "cust-123",
  "fulfilledAt": "2026-04-24T14:03:22.534Z"
}
```

### CLI flags

```
order-api
  --port                int     HTTP port (default 6080)
  --bootstrap-servers   string  Kafka bootstrap (default "localhost:9092")

fulfillment-service
  --bootstrap-servers   string  Kafka bootstrap (default "localhost:9092")

notification-service
  --bootstrap-servers   string  Kafka bootstrap (default "localhost:9092")
```

---

## 5. Out of Scope

Deferred to V2 or later:

- Idempotent producer (`enable.idempotence=true`) and exactly-once semantics
- Transactional outbox — atomic write of state + event
- Dead letter queue for poison messages
- Manual offset commits (`commitSync` / `commitAsync`)
- Saga pattern with compensating actions (e.g. payment → inventory → ship)
- Distributed tracing (deferred to Build 5)
- Prometheus metrics (deferred to V2 chaos report)
- Order persistence — V1 stores fulfilled orders in memory only
- Authentication, TLS, ACLs
- Schema registry, Avro, or Protobuf — plain JSON only in V1
- Horizontal scaling beyond one consumer per group

---

## 6. Deliberate V1 Failure Modes

V1 is designed to break in these specific ways. Each is the target of a V2 fix.

| # | Failure | How to trigger | Observable symptom | V2 fix |
|---|---------|---------------|-------------------|--------|
| F1 | Duplicate orders | Client retries `POST /orders` after a network timeout | Two records on `orders`, two fulfillments, two `OrderFulfilled` events, two notifications | Idempotent producer + client-supplied idempotency key table |
| F2 | Duplicate notifications | Kill `fulfillment-service` between producing `OrderFulfilled` and the next auto-commit tick (≤ 5 s window) | Event delivered; on restart the original `orders` record is redelivered → `OrderFulfilled` produced again → notification logged twice | Transactional outbox (atomic state + event) |
| F3 | Lost orders | Kill `fulfillment-service` between an auto-commit tick and completion of work for a record in the next poll batch | Committed offset advanced past a record that was never fulfilled | Manual `commitSync` *after* work completes |
| F4 | Poison message | `kafka-console-producer --topic orders` with malformed JSON | Deserialization exception, consumer thread dies, partition stalls; on restart the same record is re-read and crashes again | DLQ with record routing after N retry attempts |
| F5 | Consumer lag | Burst 10 000 orders against a single consumer per group | Lag grows linearly on both topics; notifications arrive minutes late | Tuning `max.poll.records`, partition parallelism, optional parallel consumer |

The V2 chaos report will consist of a timeline screenshot of Kafka lag and DLQ contents
captured while each of these failures is deliberately induced, before and after the fix.

Each failure mode has a deterministic test in `chaos-test/` that asserts the V1 buggy
behavior. The V2 implementation inverts the assertions to prove the fix:

| # | Test class | Run with |
|---|-----------|----------|
| F1 | `F1_DuplicateOrdersTest` | `make chaos` |
| F2 | `F2_DuplicateNotificationsTest` | `make chaos` |
| F3 | `F3_LostOrdersTest` | `make chaos` |
| F4 | `F4_PoisonMessageTest` | `make chaos` |
| F5 | `F5_ConsumerLagTest` | `make chaos` |

---

## 7. Resolved Decisions

| # | Question | Decision |
|---|----------|----------|
| Q1 | Maven or Gradle? | **Maven** multi-module. Universal, no wrapper config to debug across machines. |
| Q2 | Spring Kafka or plain `kafka-clients`? | **Plain `kafka-clients`.** V1→V2 pedagogy hinges on seeing auto-commit, `commitSync`, and offset management directly; Spring Kafka's `AckMode` abstracts exactly what V2 must make visible. |
| Q3 | Kafka image? | **Apache Kafka** (`apache/kafka:3.8.1`) in KRaft mode. Official image, single-node combined controller+broker, no ZooKeeper sidecar. (Bitnami was the first choice but `bitnami/kafka` tags were not published on Docker Hub at the time of build.) |
| Q4 | Fulfillment persistence? | **In-memory `Map`**. V2 introduces a database only when the transactional outbox pattern demands it. |
| Q5 | Schema format? | **Plain JSON.** Schema registry + Avro/Protobuf are a potential V3 topic. |
| Q6 | Partition count? | **3.** Demonstrates partition-level ordering and makes per-partition lag visible. |
| Q7 | `order-api` blocking ack vs fire-and-forget? | **Block on `producer.send(...).get()`** before responding 202. Keeps the HTTP contract honest — the client is told "accepted" only after Kafka has durably accepted the record. Acceptable latency cost in V1; revisit with async batching if throughput demands it in V3. |

---

## 8. Open Questions

| # | Question | Owner | Due |
|---|----------|-------|-----|
| *(none outstanding for V1)* | | | |

---

## 9. Revision History

| Version | Date | Author | Notes |
|---------|------|--------|-------|
| 0.1 | 2026-04-24 | Steve Weiland | Initial V1 draft — three services, auto-commit, no idempotency, no DLQ |
