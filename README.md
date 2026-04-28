# Order Processing System — V1

Event-driven order pipeline built on Java 21 + Apache Kafka. **V1 is deliberately
naive** to expose the failure modes that V2 will fix: producer-retry duplicates,
consumer-crash duplicate notifications, poison messages that stall partitions, and
consumer lag under load.

Full spec: [`spec.md`](./spec.md).

---

## Architecture

```
client ──POST /orders──▶ order-api ──▶ [orders] ──▶ fulfillment-service ──▶ [order-events] ──▶ notification-service
                                                          │
                                                          └─ in-memory fulfilled-order map
```

| Service | Language | Role | Topic in / out |
|---------|----------|------|---------------|
| `order-api` | Java 21 + Javalin | HTTP producer (:6080) | — / `orders` |
| `fulfillment-service` | Java 21 + kafka-clients | Consumer → producer | `orders` / `order-events` |
| `notification-service` | Java 21 + kafka-clients | Consumer (log only) | `order-events` / — |
| `kafka` | `apache/kafka:3.8.1` KRaft | Single-node broker, 3 partitions per topic | |

V1 design points intentionally missing idempotency (see §3.4, §6 of the spec):

- `enable.auto.commit=true` on both consumers
- No idempotency key table
- No DLQ for poison messages
- No transactional outbox
- Fulfillment store is an in-memory `Map` (lost on restart)

---

## Run it

### Everything in Docker

```bash
make run            # docker compose up --build
make send-order     # POST a sample order
make logs           # tail all three services
```

Expected timeline (one order):

```
order-api           "order accepted customerId=cust-demo total=24.48" orderId=<uuid>
fulfillment-service "fulfilling customerId=cust-demo partition=? offset=?" orderId=<uuid>
fulfillment-service "fulfilled storeSize=1" orderId=<uuid>
notification-service "Notification sent for order=<uuid> customer=cust-demo ..." orderId=<uuid>
```

### Kafka in Docker, services on host

```bash
make run-kafka                                # starts broker on localhost:9092
mvn -DskipTests package                       # build shaded jars
java -jar order-api/target/order-api.jar &
java -jar fulfillment-service/target/fulfillment-service.jar &
java -jar notification-service/target/notification-service.jar &
```

### CLI flags

```
order-api              --port <int>              (default 6080)
                       --bootstrap-servers <str> (default localhost:9092)
fulfillment-service    --bootstrap-servers <str> (default localhost:9092)
notification-service   --bootstrap-servers <str> (default localhost:9092)
```

Environment equivalents: `ORDER_API_PORT`, `KAFKA_BOOTSTRAP_SERVERS`.

---

## Inspect Kafka

```bash
make topics          # list topics
make consume-orders  # stream raw orders topic
make consume-events  # stream raw order-events topic
```

---

## Build / test

```bash
make build    # mvn compile
make test     # mvn test (unit tests only — no broker required)
make chaos    # mvn -P chaos test (Testcontainers spins up Kafka; runs F1-F5)
make package  # produce shaded jars in each service's target/
make clean    # mvn clean + docker compose down -v
```

The chaos suite (`chaos-test/`) is opt-in via the `chaos` Maven profile so the
unit-test loop stays fast (~1s). It uses Testcontainers + `apache/kafka:3.8.1`
and runs in ~35s.

---

## V1 failure modes

Each failure mode in `spec.md` §6 has a deterministic chaos test in
`chaos-test/src/test/java/com/steveweiland/orders/chaos/`. **Each test asserts the
V1 buggy behavior.** When V2 lands, the assertions invert — the diff itself is
the portfolio asset.

| # | Failure | Test class | V1 asserts | V2 will assert |
|---|---------|------------|------------|---------------|
| F1 | Duplicate orders | `F1_DuplicateOrdersTest` | 2 events for 1 logical order | 1 event (idempotency-key table) |
| F2 | Duplicate notifications | `F2_DuplicateNotificationsTest` | redelivery on restart → 2 events | 1 event (transactional outbox) |
| F3 | Lost orders | `F3_LostOrdersTest` | record skipped after pre-committed offset → 0 events | 1 event (manual commitSync after work) |
| F4 | Poison message | `F4_PoisonMessageTest` | consumer crashes; partition stalled → 0 valid events | 1 event (DLQ routes the poison) |
| F5 | Consumer lag | `F5_ConsumerLagTest` | lag > 200 records after 2s burst | lag near 0 (parallel consumer / tuning) |

`make chaos` runs the suite and prints `chaos-test/target/lag-v1.txt` — the F5
baseline number that V2 will need to beat.

V2 chaos report will combine these test outputs with Kafka lag graphs + DLQ
contents captured during the V2 implementation.

---

## What's next (V2)

- Idempotent producer + client idempotency key → fixes F1
- Transactional outbox → fixes F2
- Manual `commitSync` after work → fixes F3
- Dead letter queue → fixes F4
- `max.poll.records` tuning + parallel consumer → fixes F5
- Saga pattern (payment → inventory → ship) — stretch

---

## Reading

- Kleppmann, *Designing Data-Intensive Applications* — chapters 11–12 (stream processing, consistency & consensus)
- Confluent — *Exactly Once Semantics Are Possible: Here's How Kafka Does It*
- Chris Richardson — *Pattern: Saga*
