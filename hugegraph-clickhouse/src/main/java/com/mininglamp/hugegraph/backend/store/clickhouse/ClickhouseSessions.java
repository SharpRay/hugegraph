package com.mininglamp.hugegraph.backend.store.clickhouse;

import com.baidu.hugegraph.backend.store.BackendSession;
import com.baidu.hugegraph.backend.store.BackendSessionPool;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class ClickhouseSessions extends BackendSessionPool {

    public class Session extends BackendSession.AbstractBackendSession {

        private Connection conn;
        private Map<String, PreparedStatement> statements;
        private int count;

        public Session() {
            this.conn = null;
            this.statements = new HashMap<>();
            this.count = 0;
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

        public boolean execute(String sql) throws SQLException {
            return execute(sql, null);
        }
    }
}
