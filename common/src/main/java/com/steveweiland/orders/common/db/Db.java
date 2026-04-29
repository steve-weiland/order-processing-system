package com.steveweiland.orders.common.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public final class Db {
    private Db() {}

    public static HikariDataSource pool(String jdbcUrl, String user, String password, String poolName, int maxPool) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(jdbcUrl);
        cfg.setUsername(user);
        cfg.setPassword(password);
        cfg.setPoolName(poolName);
        cfg.setMaximumPoolSize(maxPool);
        cfg.setConnectionTimeout(10_000);
        cfg.setInitializationFailTimeout(30_000);
        cfg.setAutoCommit(true);
        return new HikariDataSource(cfg);
    }

    public static DataSource pool(String jdbcUrl, String user, String password, String poolName) {
        return pool(jdbcUrl, user, password, poolName, 10);
    }
}
