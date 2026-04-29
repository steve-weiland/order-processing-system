package com.steveweiland.orders.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.steveweiland.orders.common.JsonMapper;
import com.steveweiland.orders.common.Order;
import com.steveweiland.orders.common.db.Db;
import com.steveweiland.orders.common.db.Migrations;
import com.steveweiland.orders.common.topics.TopicAdmin;
import com.zaxxer.hikari.HikariDataSource;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.HttpStatus;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class OrderApiApp {
    private static final Logger log = LoggerFactory.getLogger(OrderApiApp.class);
    private static final ObjectMapper JSON = JsonMapper.shared();

    private OrderApiApp() {}

    public static void main(String[] args) throws Exception {
        int port = intFlag(args, "--port", envInt("ORDER_API_PORT", 6080));
        String bootstrap = strFlag(args, "--bootstrap-servers",
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        String jdbcUrl = strFlag(args, "--jdbc-url",
                System.getenv().getOrDefault("JDBC_URL", "jdbc:postgresql://localhost:5432/orders"));
        String jdbcUser = strFlag(args, "--jdbc-user",
                System.getenv().getOrDefault("JDBC_USER", "orders"));
        String jdbcPassword = strFlag(args, "--jdbc-password",
                System.getenv().getOrDefault("JDBC_PASSWORD", "orders"));

        TopicAdmin.ensure(bootstrap, List.of(
                new NewTopic("orders", 3, (short) 1),
                new NewTopic("order-events", 3, (short) 1),
                new NewTopic("orders.dlq", 3, (short) 1)));

        HikariDataSource ds = Db.pool(jdbcUrl, jdbcUser, jdbcPassword, "order-api-pool", 8);
        Migrations.migrate(ds);

        IdempotencyKeyStore idempStore = new IdempotencyKeyStore(ds);
        OrderProducer producer = new OrderProducer(bootstrap);
        Javalin app = Javalin.create(cfg -> cfg.showJavalinBanner = false);

        app.get("/health", ctx -> ctx.json(Map.of("status", "ok")));
        app.post("/orders", ctx -> handleCreateOrder(ctx, producer, idempStore));

        app.start(port);
        log.info("order-api listening on :{} bootstrap={} jdbc={}", port, bootstrap, jdbcUrl);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down");
            app.stop();
            producer.close();
            ds.close();
        }, "order-api-shutdown"));
    }

    private static void handleCreateOrder(Context ctx, OrderProducer producer, IdempotencyKeyStore idempStore) {
        OrderRequest req;
        try {
            req = JSON.readValue(ctx.body(), OrderRequest.class);
        } catch (Exception e) {
            ctx.status(HttpStatus.BAD_REQUEST).json(Map.of("error", "invalid JSON: " + e.getMessage()));
            return;
        }

        String err = OrderValidator.validate(req);
        if (err != null) {
            ctx.status(HttpStatus.BAD_REQUEST).json(Map.of("error", err));
            return;
        }

        String idempotencyKey = ctx.header("Idempotency-Key");
        String orderId;
        boolean shouldProduce;
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            IdempotencyKeyStore.Decision dec = idempStore.claim(idempotencyKey, UUID.randomUUID().toString());
            orderId = dec.orderId();
            shouldProduce = !dec.isReplay();
            if (dec.isReplay()) {
                MDC.put("orderId", orderId);
                try {
                    log.info("idempotent replay key={}", idempotencyKey);
                } finally {
                    MDC.remove("orderId");
                }
            }
        } else {
            orderId = UUID.randomUUID().toString();
            shouldProduce = true;
        }

        MDC.put("orderId", orderId);
        try {
            if (shouldProduce) {
                BigDecimal total = OrderValidator.total(req);
                Order order = new Order(orderId, req.customerId(), req.items(), total, Instant.now());
                producer.send(order);
                log.info("order accepted customerId={} total={}", req.customerId(), total);
            }
            ctx.status(HttpStatus.ACCEPTED).json(Map.of("orderId", orderId));
        } catch (Exception e) {
            log.error("failed to publish order", e);
            ctx.status(HttpStatus.INTERNAL_SERVER_ERROR).json(Map.of("error", "failed to publish order"));
        } finally {
            MDC.remove("orderId");
        }
    }

    private static String strFlag(String[] args, String name, String def) {
        for (int i = 0; i < args.length - 1; i++) {
            if (name.equals(args[i])) return args[i + 1];
        }
        return def;
    }

    private static int intFlag(String[] args, String name, int def) {
        String v = strFlag(args, name, null);
        return v == null ? def : Integer.parseInt(v);
    }

    private static int envInt(String name, int def) {
        String v = System.getenv(name);
        return v == null ? def : Integer.parseInt(v);
    }
}
