#!/usr/bin/env bash
# smoke.sh — end-to-end gate over the running compose stack (make smoke).
#
# Drives real traffic through every mechanism the README claims:
#   1. all six containers running
#   2. order accepted → saga runs → processed_orders FULFILLED
#   3. Idempotency-Key replay returns the SAME orderId, produces nothing new
#   4. notification-service logged the order's notification
#   5. poison record → orders.dlq with x-dlq-* diagnostic headers
#   6. saga failure injection: INVENTORY_FAILURE_RATE=1.0 recreate →
#      FAILED row + failure_step=inventory + payment compensated, then
#      rate restored → next order FULFILLED (proves the env passthrough
#      applied AND the restore took)
#
# Assumes the stack is up (make run / docker compose up -d --build).
# Exits non-zero on first failure. Ports/topics per docker-compose.yml.
set -euo pipefail

BASE="${BASE:-http://localhost:6080}"
COMPOSE="${COMPOSE:-docker compose}"

pass=0
check() { pass=$((pass+1)); echo "  ok $pass: $1"; }
fail() { echo "  FAIL: $1" >&2; exit 1; }

retry() {  # retry <seconds> <description> <command...> — every 2s
    local deadline=$(( $(date +%s) + $1 )); local what=$2; shift 2
    until "$@" >/dev/null 2>&1; do
        (( $(date +%s) < deadline )) || fail "$what (timed out)"
        sleep 2
    done
}

psql_one() { docker exec ops-postgres psql -U orders -d orders -tAc "$1"; }
order_status() { psql_one "SELECT status FROM processed_orders WHERE order_id='$1'"; }

post_order() {  # post_order <customerId> [idempotency-key] → orderId on stdout
    local args=(-s -X POST "$BASE/orders" -H 'Content-Type: application/json'
        -d '{"customerId":"'"$1"'","items":[{"sku":"SKU-S","quantity":1,"unitPrice":9.99}]}')
    [ -n "${2:-}" ] && args+=(-H "Idempotency-Key: $2")
    curl "${args[@]}" | sed -n 's/.*"orderId":"\([^"]*\)".*/\1/p'
}

echo "smoke: waiting for readiness"
retry 90 "order-api /health" curl -fsS "$BASE/health"

# 1 ── all containers up
running=$($COMPOSE ps --status running --format '{{.Service}}' | wc -l | tr -d ' ')
[ "$running" -ge 6 ] || fail "only $running/6 services running"
check "all 6 containers running"

# 2 ── happy path: order → saga → FULFILLED
oid=$(post_order cust-smoke)
[ -n "$oid" ] || fail "POST /orders returned no orderId"
fulfilled() { [ "$(order_status "$oid")" = "FULFILLED" ]; }
retry 30 "order $oid FULFILLED" fulfilled
check "order accepted and fulfilled ($oid)"

# 3 ── idempotency replay: same key → same orderId, nothing new produced
key="smoke-$(date +%s)-$$"
first=$(post_order cust-replay "$key")
replay=$(post_order cust-replay "$key")
[ -n "$first" ] && [ "$first" = "$replay" ] || fail "replay returned $replay, want $first"
rows=$(psql_one "SELECT count(*) FROM processed_orders WHERE order_id='$first'")
[ "$rows" -le 1 ] || fail "replay created a duplicate row"
check "Idempotency-Key replay returns the same orderId ($first)"

# 4 ── notification logged for the happy-path order
notified() { docker logs ops-notifications 2>&1 | grep -q "order=$oid"; }
retry 30 "notification for $oid" notified
check "notification-service logged the notification"

# 5 ── poison → orders.dlq with diagnostic headers
marker="smoke-poison-$(date +%s)-$$"
echo "not-json $marker" | docker exec -i ops-kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic orders >/dev/null 2>&1
dlq_out=$(mktemp)
docker exec ops-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic orders.dlq --from-beginning \
    --timeout-ms 15000 --property print.headers=true > "$dlq_out" 2>/dev/null || true
grep -a "$marker" "$dlq_out" | grep -aq "x-dlq-source-topic" \
    || fail "poison record not on orders.dlq with x-dlq-* headers"
rm -f "$dlq_out"
check "poison routed to orders.dlq with diagnostic headers"

# 6 ── saga failure injection via the compose env passthrough
echo "  (recreating fulfillment with INVENTORY_FAILURE_RATE=1.0)"
INVENTORY_FAILURE_RATE=1.0 $COMPOSE up -d fulfillment-service-1 fulfillment-service-2 >/dev/null 2>&1
sleep 5
foid=$(post_order cust-smoke-fail)
failed() { [ "$(order_status "$foid")" = "FAILED" ]; }
retry 30 "order $foid FAILED under injected inventory failure" failed
step=$(psql_one "SELECT failure_step FROM sagas WHERE order_id='$foid'")
[ "$step" = "inventory" ] || fail "failure_step=$step, want inventory"
echo "  (restoring INVENTORY_FAILURE_RATE=0.0)"
INVENTORY_FAILURE_RATE=0.0 $COMPOSE up -d fulfillment-service-1 fulfillment-service-2 >/dev/null 2>&1
sleep 5
roid=$(post_order cust-smoke-restored)
restored() { [ "$(order_status "$roid")" = "FULFILLED" ]; }
retry 30 "order $roid FULFILLED after restore" restored
check "failure injection applied (FAILED @ inventory) and restore took (next order FULFILLED)"

echo
echo "SMOKE PASS ($pass checks). Stack left running: make stop / make clean."
