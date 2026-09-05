.PHONY: build test chaos smoke package run run-local run-kafka run-postgres logs stop clean order send-order topics psql consume-orders consume-events consume-dlq consume-events-dlq

build:
	mvn -ntp compile

test:
	mvn -ntp test

chaos:
	mvn -ntp -P chaos -pl chaos-test -am test
	@echo
	@echo '=== chaos-test/target/lag-v2.1.txt ==='
	@cat chaos-test/target/lag-v2.1.txt 2>/dev/null || echo '(not produced — F5 may have failed)'

# End-to-end gate over the running stack (make run first): order flow,
# idempotency replay, notification, DLQ headers, saga failure injection.
smoke:
	@./scripts/smoke.sh

package:
	mvn -ntp -DskipTests package

run:
	docker compose up --build

run-kafka:
	docker compose up -d kafka

run-postgres:
	docker compose up -d postgres

# Postgres is published on host port 6432 (docker-compose maps 6432:5432 to
# avoid clashing with a host Postgres), so host-run services need an explicit
# --jdbc-url; the in-container default targets port 5432.
run-local: run-kafka run-postgres
	@echo "Kafka on localhost:9092, Postgres on localhost:6432. In separate terminals:"
	@echo "  java -jar order-api/target/order-api.jar          --jdbc-url jdbc:postgresql://localhost:6432/orders"
	@echo "  java -jar fulfillment-service/target/fulfillment-service.jar --jdbc-url jdbc:postgresql://localhost:6432/orders"
	@echo "  java -jar notification-service/target/notification-service.jar --jdbc-url jdbc:postgresql://localhost:6432/orders"

logs:
	docker compose logs -f order-api fulfillment-service-1 fulfillment-service-2 notification-service

# Containers go, the pgdata volume stays — orders survive the next `make run`.
stop:
	docker compose down

# Full wipe: build output AND volumes (pgdata included).
clean:
	mvn -ntp clean
	docker compose down -v 2>/dev/null || true

send-order:
	@curl -s -X POST http://localhost:6080/orders \
	  -H 'Content-Type: application/json' \
	  -d '{"customerId":"cust-demo","items":[{"sku":"SKU-A","quantity":2,"unitPrice":9.99},{"sku":"SKU-B","quantity":1,"unitPrice":4.50}]}' \
	  | tee /dev/stderr; echo

topics:
	docker exec ops-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

consume-orders:
	docker exec -it ops-kafka /opt/kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server localhost:9092 --topic orders --from-beginning

consume-events:
	docker exec -it ops-kafka /opt/kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server localhost:9092 --topic order-events --from-beginning

consume-dlq:
	docker exec -it ops-kafka /opt/kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server localhost:9092 --topic orders.dlq --from-beginning --property print.headers=true

consume-events-dlq:
	docker exec -it ops-kafka /opt/kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server localhost:9092 --topic order-events.dlq --from-beginning --property print.headers=true

psql:
	docker exec -it ops-postgres psql -U orders -d orders
