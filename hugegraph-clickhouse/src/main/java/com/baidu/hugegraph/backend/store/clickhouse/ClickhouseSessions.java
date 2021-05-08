package com.baidu.hugegraph.backend.store.clickhouse;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.BackendSession;
import com.baidu.hugegraph.backend.store.BackendSessionPool;
import com.baidu.hugegraph.backend.store.mysql.MysqlUtil;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Log;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;

import java.net.SocketTimeoutException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

public class ClickhouseSessions extends BackendSessionPool {

    private static final Logger LOG = Log.logger(ClickhouseSessions.class);

    private static final int DROP_DB_TIMEOUT = 10000;

    private HugeConfig config;
    private String database;
    private volatile boolean opened;

    public ClickhouseSessions(HugeConfig config, String database, String store) {
        super(config, database + "/" + store);
        this.config = config;
        this.database = database;
        this.opened = false;
    }

    public String database() {
        return this.database;
    }

    @Override
    public synchronized  void open() throws Exception {
        try (Connection conn = this.open(false)) {
            this.opened = true;
        }
    }

    private Connection open(boolean autoReconnect) throws SQLException {
        String url = this.buildUri(true, true, autoReconnect, null);
        return this.connect(url);
    }

    @Override
    protected boolean opened() {
        return this.opened;
    }

    @Override
    public Session session() {
        return (Session) super.getOrNewSession();
    }

    @Override
    protected void doClose() {
        // pass
    }

    @Override
    protected Session newSession() {
        return new Session();
    }

    public void createDatabase() {
        // Create databasw with non-database-session
        LOG.debug("Create database: {}", this.database());

        String sql = this.buildCreateDatabase(this.database());
        try (Connection conn = this.openWithoutDB(0)) {
            conn.createStatement().execute(sql);
        } catch (SQLException e) {
            if (!e.getMessage().endsWith("already exists")) {
                throw new BackendException("Failed to create database '%s'",
                        e, this.database());
            }
            // Ignore exception if database already exists
        }
    }

    public void dropDatabase() {
        LOG.debug("Drop database: {}", this.database());

        String sql = this.buildDropDatabase(this.database());
        try (Connection conn = this.openWithoutDB(DROP_DB_TIMEOUT)) {
            conn.createStatement().execute(sql);
        } catch (SQLException e) {
            if (e.getCause() instanceof SocketTimeoutException) {
                LOG.warn("Drop database '{}' timeout", this.database());
            } else {
                throw new BackendException("Failed to drop database '%s'",
                        e, this.database());
            }
        }
    }

    public boolean existsDatabase() {
        try (Connection conn = this.openWithoutDB(0);
             ResultSet result = conn.getMetaData().getCatalogs()) {
            while (result.next()) {
                String dbName = result.getString(1);
                if (dbName.equals(this.database())) {
                    return true;
                }
            }
        } catch (Exception e) {
            throw new BackendException("Failed to obtain database info", e);
        }
        return false;
    }

    public void resetConnections() {
        // Close the under layer connections owned by each thread
        this.forceResetSessions();
    }

    public boolean existsTable(String table) {
        String sql = this.buildExistsTable(table);
        try (Connection conn = this.openWithDB(0);
             ResultSet result = conn.createStatement().executeQuery(sql)) {
            return result.next();
        } catch (Exception e) {
            throw new BackendException("Failed to obtain table info", e);
        }
    }

    /**
     * Connect DB with specified database, but won't auto reconnect.
     */
    protected Connection openWithDB(int timeout) {
        String url = this.buildUri(false, true, false, timeout);
        try {
            return this.connect(url);
        } catch (SQLException e) {
            throw new BackendException("Failed to access %s", e, url);
        }
    }

    /**
     * Connect DB without specified database
     */
    protected Connection openWithoutDB(int timeout) {
        String url = this.buildUri(false, false, false, timeout);
        try {
            return this.connect(url);
        } catch (SQLException e) {
            throw new BackendException("Failed to access %s", e, url);
        }
    }

    protected String buildCreateDatabase(String database) {
        return String.format("CREATE DATABASE IF NOT EXISTS %s",
                database);
    }

    protected String buildDropDatabase(String database) {
        return String.format("DROP DATABASE IF EXISTS %s;", database);
    }

    protected String buildExistsTable(String table) {
        return String.format("SELECT * FROM system.tables " +
                "WHERE database = '%s' AND " +
                "name = '%s' LIMIT 1;",
                this.escapedDatabase(), MysqlUtil.escapeString(table));
    }

    public String escapedDatabase() {
        return MysqlUtil.escapeString(this.database());
    }

    protected String buildUri(boolean withConnParams, boolean withDB,
                              boolean autoReconnect, Integer timeout) {
        String url = this.buildUrlPrefix(withDB);

        int maxTimes = this.config.get(ClickhouseOptions.JDBC_RECONNECT_MAX_TIMES);
        int interval = this.config.get(ClickhouseOptions.JDBC_RECONNECT_INTERVAL);
        String sslMode = this.config.get(ClickhouseOptions.JDBC_SSL_MODE);

        URIBuilder builder = this.newConnectionURIBuilder();
        builder.setPath(url).setParameter("useSSL",sslMode);
        if (withConnParams) {
            builder.setParameter("characterEncoding", "utf-8")
                    .setParameter("rewriteBatchedStatements", "true")
                    .setParameter("useServerPrepStmts", "false")
                    .setParameter("autoReconnect", String.valueOf(autoReconnect))
                    .setParameter("maxReconnects", String.valueOf(maxTimes))
                    .setParameter("initialTimeout", String.valueOf(interval));
        }
        if (timeout != null) {
            builder.setParameter("socketTimeput", String.valueOf(timeout));
        }
        return builder.toString();
    }

    protected URIBuilder newConnectionURIBuilder() {
        return new URIBuilder();
    }

    protected String buildUrlPrefix(boolean withDB) {
        String url = this.config.get(ClickhouseOptions.JDBC_URL);
        if (!url.endsWith("/")) {
            url = String.format("%s/", url);
        }
        String database = withDB ? this.database() : this.connectDatabase();
        return String.format("%s%s", url, database);
    }

    protected String connectDatabase() {
        return Strings.EMPTY;
    }

    private Connection connect(String url) throws SQLException {
        String driverName = this.config.get(ClickhouseOptions.JDBC_DRIVER);
        String username = this.config.get(ClickhouseOptions.JDBC_USERNAME);
        String password = this.config.get(ClickhouseOptions.JDBC_PASSWORD);
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            throw new BackendException("Invalid driver class '%s'",
                    driverName);
        }
        return DriverManager.getConnection(url, username, password);
    }

    public class Session extends BackendSession.AbstractBackendSession {

        private Connection conn;
        private Map<String, PreparedStatement> statements;
        private int count;

        public Session() {
            this.conn = null;
            this.statements = new HashMap<>();
            this.count = 0;
        }

        @Override
        public void open() {
            try {
                this.doOpen();
            } catch (SQLException e) {
                throw new BackendException("Failed to open connection", e);
            }
        }

        private void doOpen() throws SQLException {
            if (this.conn != null && !this.conn.isClosed()) {
                return;
            }
            this.conn = ClickhouseSessions.this.open(true);
            this.opened = true;
        }

        private void tryOpen() {
            try {
                this.doOpen();
            } catch (SQLException ignored) {
                // Ignore
            }
        }

        @Override
        public void close() {
            assert this.closeable();
            if (this.conn == null) {
                return;
            }

            this.opened = false;
            this.doClose();
        }

        private void doClose() {
            SQLException exception = null;
            for (PreparedStatement statement : this.statements.values()) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    exception = e;
                }
            }
            this.statements.clear();

            try {
                this.conn.close();
            } catch (SQLException e) {
                exception = e;
            } finally {
                this.conn = null;
            }

            if (exception != null) {
                throw new BackendException("Failed to close connection",
                        exception);
            }
        }

        @Override
        public boolean opened() {
            if (this.opened && this.conn == null) {
                // Reconnect if the connection is reset
                tryOpen();
            }
            return this.opened && this.conn != null;
        }

        @Override
        public boolean closed() {
            if (!this.opened || this.conn == null) {
                return true;
            }
            try {
                return this.conn.isClosed();
            } catch (SQLException ignored) {
                // Assume closed here
                return true;
            }
        }

        public void clear() {
            this.count = 0;
            SQLException exception = null;
            for (PreparedStatement statement : this.statements.values()) {
                try {
                    statement.clearBatch();
                } catch (SQLException e) {
                    exception = e;
                }
            }
            if (exception != null) {
                /*
                 * Will throw exception when the database connection error,
                 * we clear statements because clearBatch() failed
                 */
                this.statements = new HashMap<>();
            }
        }

        public ResultSet select(String sql) throws SQLException {
            // Clickhouse do not support transactions for now
            return this.conn.createStatement().executeQuery(sql);
        }

        public boolean execute(String sql, String table) throws SQLException {
            /*
             * Clickhouse do not support transactions for now,
             * but from version 21.1 the WAL + fsync is supported.
             * TODO WAL and fsync configurations should be provided
             */
            boolean res = this.conn.createStatement().execute(sql);

            if (StringUtils.isNotEmpty(table)) {
                sql = String.format("OPTIMIZE TABLE %s FINAL", table);
                execute(sql, null);
            }
            return res;
        }

        public void begin() throws SQLException {
        }

        public void end() throws SQLException {
        }

        public void endAndLog() {

        }

        @Override
        public Integer commit() {
            int updated = 0;
            try {
                for (PreparedStatement statement : this.statements.values()) {
                    updated += IntStream.of(statement.executeBatch()).sum();
                }
                this.conn.commit();
                this.clear();
            } catch (SQLException e) {
                throw new BackendException("Failed to commit", e);
            }
            this.endAndLog();
            return updated;
        }

        @Override
        public void rollback() {
            this.clear();
            throw new UnsupportedOperationException("Transaction is not supported by Clickhouse");
        }

        @Override
        public boolean hasChanges() {
            return this.count > 0;
        }

        @Override
        public void reconnectIfNeeded() {
            if (!this.opened) {
                return;
            }

            if (this.conn == null) {
                tryOpen();
            }

            try {
                this.execute("SELECT 1;");
            } catch (SQLException ignored) {
                // pass
            }
        }

        @Override
        public void reset() {
            // NOTE: this method mauy by called by other threads
            if (this.conn == null) {
                return;
            }
            try {
                this.doClose();
            } catch (Exception e) {
                LOG.warn("Failed to reset connection", e);
            }
        }

        public boolean execute(String sql) throws SQLException {
            return execute(sql, null);
        }

        public void add(PreparedStatement stmt) {
            try {
                // Add a row to statement
                stmt.addBatch();
                this.count++;
            } catch (SQLException e) {
                throw new BackendException("Failed to add statement '%s' " +
                        "to batch", e, stmt);
            }
        }

        public PreparedStatement prepareStatement(String sqlTemplate) throws SQLException {
            PreparedStatement stmt = this.statements.get(sqlTemplate);
            if (stmt == null) {
                stmt = this.conn.prepareStatement(sqlTemplate);
                this.statements.putIfAbsent(sqlTemplate, stmt);
            }
            return stmt;
        }
    }
}
