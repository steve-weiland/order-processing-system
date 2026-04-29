package com.steveweiland.orders.common.db;

import org.flywaydb.core.Flyway;

import javax.sql.DataSource;

public final class Migrations {
    private Migrations() {}

    /**
     * Run Flyway migrations from classpath:db/migration. Safe to call concurrently
     * from multiple services — Flyway uses a DB-level lock on its history table.
     */
    public static void migrate(DataSource ds) {
        Flyway.configure()
                .dataSource(ds)
                .locations("classpath:db/migration")
                .load()
                .migrate();
    }
}
