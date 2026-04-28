.PHONY: build test chaos package run run-local run-kafka logs stop clean order send-order topics

build:
	mvn -ntp compile

test:
	mvn -ntp test

chaos:
	mvn -ntp -P chaos -pl chaos-test -am test
	@echo
	@echo '=== chaos-test/target/lag-v1.txt ==='
	@cat chaos-test/target/lag-v1.txt 2>/dev/null || echo '(not produced — F5 may have failed)'

package:
	mvn -ntp -DskipTests package

run:
	docker compose up --build

run-kafka:
	docker compose up -d kafka

run-local: run-kafka
	@echo "Kafka up on localhost:9092. In separate terminals:"
	@echo "  java -jar order-api/target/order-api.jar"
	@echo "  java -jar fulfillment-service/target/fulfillment-service.jar"
	@echo "  java -jar notification-service/target/notification-service.jar"

logs:
	docker compose logs -f order-api fulfillment-service notification-service

stop:
	docker compose down

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
