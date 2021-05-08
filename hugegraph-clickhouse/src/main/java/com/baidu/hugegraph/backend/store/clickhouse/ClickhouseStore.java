package com.baidu.hugegraph.backend.store.clickhouse;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.*;
import com.baidu.hugegraph.backend.store.mysql.MysqlMetrics;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.exception.ConnectionException;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import org.slf4j.Logger;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ClickhouseStore extends AbstractBackendStore<ClickhouseSessions.Session> {

    private static final Logger LOG = Log.logger(ClickhouseStore.class);

    private static BackendFeatures FEATURES = new ClickhouseFeatures();

    private final String store;
    private final String database;

    private final BackendStoreProvider provider;

    private final Map<HugeType, ClickhouseTable> tables;

    private ClickhouseSessions sessions;

    public ClickhouseStore(final BackendStoreProvider provider,
                           final String database, final String store) {
        E.checkNotNull(database, "database");
        E.checkNotNull(store, store);
        this.provider = provider;
        this.database = database;
        this.store = store;

        this.sessions = null;
        this.tables = new ConcurrentHashMap<>();
    }

    private void registerMetaHandlers() {
        this.registerMetaHandler("metrics", (session, meta, args) -> {
            MysqlMetrics metrics = new MysqlMetrics();
            return metrics.getMetrics();
        });
    }

    protected ClickhouseSessions openSessionPool(HugeConfig config) {
        return new ClickhouseSessions(config, this.database, this.store);
    }

    @Override
    /**
     * Not really open db.
     */
    public synchronized void open(HugeConfig config) {
        LOG.debug("Store open: {}", this.store);

        E.checkNotNull(config, "config");

        if (this.sessions != null && !this.sessions.closed()) {
            LOG.debug("Store {} has been opened before", this.store);
            this.sessions.useSession();
            return;
        }

        this.sessions = this.openSessionPool(config);

        LOG.debug("Store connect with database: {}", this.database);
        try {
            this.sessions.open();
        } catch (Exception e) {
            // This exception must be thrown because the db has not created
            if (!e.getMessage().matches("^.*Database .+ doesn't exist .*\\s*$")) {
                throw new ConnectionException("Failed to connect to MySQL", e);
            }
            if (this.isSchemaStore()) {
                LOG.info("Failed to open database '{}', " +
                        "try to init database later", this.database);
            }
        }

        try {
            this.sessions.session();
        } catch (Throwable e) {
            try {
                this.sessions.close();
            } catch (Throwable e2) {
                LOG.warn("Failed to close connection after an error", e2);
            }
            throw new BackendException("Failed to open database", e);
        }

        LOG.debug("Store opened: {}", this.store);
    }

    protected final void checkClusterConnected() {
        E.checkState(this.sessions != null,
                "Clickhouse store has not been initialized");
    }

    @Override
    protected final ClickhouseTable table(HugeType type) {
        assert type != null;
        ClickhouseTable table = this.tables.get(type);
        if (table == null) {
            throw new BackendException("Unsupported table type: %s", type);
        }
        return table;
    }

    @Override
    public String store() {
        return this.store;
    }

    @Override
    public String database() {
        return this.database;
    }

    @Override
    public BackendStoreProvider provider() {
        return this.provider;
    }

    @Override
    protected ClickhouseSessions.Session session(HugeType type) {
        this.checkOpened();
        return this.sessions.session();
    }

    @Override
    public void close() {
        LOG.debug("Store close: {}", this.store);
        this.checkClusterConnected();
        this.sessions.close();
    }

    @Override
    public boolean opened() {
        this.checkClusterConnected();
        return this.sessions.session().opened();
    }

    @Override
    public void init() {
        this.checkClusterConnected();
        this.sessions.createDatabase();
        try {
            this.sessions.session().open();
        } catch (Exception e) {
            throw new BackendException("Failed to connect database '%s'",
                    this.database);
        }
        this.checkOpened();
        this.initTables();

        LOG.debug("Store initialized: {}", this.store);
    }

    @Override
    public void clear(boolean clearSpace) {
        // Check connected
        this.checkClusterConnected();

        if (this.sessions.existsDatabase()) {
            if (!clearSpace) {
                this.checkOpened();
                this.clearTables();
                this.sessions.resetConnections();
            } else {
                this.sessions.dropDatabase();
            }
        }

        LOG.debug("Store cleared: {}", this.store);
    }

    @Override
    public boolean initialized() {
        this.checkClusterConnected();

        if (!this.sessions.existsDatabase()) {
            return false;
        }
        for (ClickhouseTable table : this.tables()) {
            if (!this.sessions.existsTable(table.table())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void truncate() {
        this.checkOpened();

        this.truncateTables();
        LOG.debug("Store truncated: {}", this.store);
    }

    @Override
    public void mutate(BackendMutation mutation) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} mutation: {}", this.store, mutation);
        }

        this.checkOpened();
        ClickhouseSessions.Session session = this.sessions.session();

        for (Iterator<BackendAction> it = mutation.mutation(); it.hasNext();) {
            this.mutate(session, it.next());
        }
    }

    private void mutate(ClickhouseSessions.Session session, BackendAction item) {
        ClickhouseBackendEntry entry = castBackendEntry(item.entry());
        ClickhouseTable table = this.table(entry.type());

        switch (item.action()) {
            case INSERT:
                table.insert(session, entry.row());
                break;
            case DELETE:
                table.delete(session, entry.row());
                break;
            case APPEND:
                table.append(session, entry.row());
                break;
            case ELIMINATE:
                table.eliminate(session, entry.row());
                break;
            default:
                throw new AssertionError(String.format(
                        "Unsupported mutate action: %s", item.action()));
        }
    }

    @Override
    public Iterator<BackendEntry> query(Query query) {
        this.checkOpened();

        ClickhouseTable table = this.table(ClickhouseTable.tableType(query));
        return table.query(this.sessions.session(), query);
    }

    @Override
    public Number queryNumber(Query query) {
        this.checkOpened();

        ClickhouseTable table = this.table(ClickhouseTable.tableType(query));
        return table.queryNumber(this.sessions.session(), query);
    }

    @Override
    public void beginTx() {
        this.checkOpened();

        ClickhouseSessions.Session session = this.sessions.session();
        try {
            session.begin();
        } catch (SQLException e) {
            throw new BackendException("Failed to open transaction", e);
        }
    }

    @Override
    public void commitTx() {
        this.checkOpened();

        ClickhouseSessions.Session session = this.sessions.session();
        int count = session.commit();
        LOG.debug("Store {} committed {} items", this.store, count);
    }

    @Override
    public void rollbackTx() {
        this.checkOpened();

        ClickhouseSessions.Session session = this.sessions.session();
        session.rollback();
    }

    @Override
    public BackendFeatures features() {
        return FEATURES;
    }


    protected static ClickhouseBackendEntry castBackendEntry(BackendEntry entry) {
        if (!(entry instanceof BackendEntry)) {
            throw new BackendException(
                    "Clickhouse store only supports ClickhouseBackendEntry");
        }
        return (ClickhouseBackendEntry) entry;
    }

    protected void clearTables() {
        ClickhouseSessions.Session session = this.sessions.session();
        for (ClickhouseTable table : this.tables()) {
            table.clear(session);
        }
    }

    protected void truncateTables() {
        ClickhouseSessions.Session session = this.sessions.session();
        for (ClickhouseTable table : this.tables()) {
            table.truncate(session);
        }
    }

    protected void initTables() {
        ClickhouseSessions.Session session = this.sessions.session();
        // Create tables
        this.tables().forEach(table -> table.init(session));
    }

    protected Collection<ClickhouseTable> tables() {
        return this.tables.values();
    }

    protected void registerTableManager(HugeType type, ClickhouseTable table) {
        this.tables.put(type, table);
    }

    public static class ClickhouseSchemaStore extends ClickhouseStore {

        private final ClickhouseTables.Counters counters;

        public ClickhouseSchemaStore(BackendStoreProvider provider,
                                     String database, String store) {
            super(provider, database, store);

            this.counters = new ClickhouseTables.Counters();

            registerTableManager(HugeType.VERTEX_LABEL,
                    new ClickhouseTables.VertexLabel());
            registerTableManager(HugeType.EDGE_LABEL,
                    new ClickhouseTables.EdgeLabel());
            registerTableManager(HugeType.PROPERTY_KEY,
                    new ClickhouseTables.PropertyKey());
            registerTableManager(HugeType.INDEX_LABEL,
                    new ClickhouseTables.IndexLabel());

        }

        @Override
        protected Collection<ClickhouseTable> tables() {
            List<ClickhouseTable> tables = new ArrayList<>(super.tables());
            tables.add(this.counters);
            return tables;
        }

        @Override
        public void increaseCounter(HugeType type, long increment) {
            this.checkOpened();
            ClickhouseSessions.Session session = super.sessions.session();
            this.counters.increaseCounter(session, type, increment);
        }

        @Override
        public long getCounter(HugeType type) {
            this.checkOpened();
            ClickhouseSessions.Session session = super.sessions.session();
            return this.counters.getCounter(session, type);
        }

        @Override
        public boolean isSchemaStore() {
            return true;
        }
    }

    public static class ClickhouseGraphStore extends ClickhouseStore {

        public ClickhouseGraphStore(BackendStoreProvider provider,
                                    String database, String store) {
            super(provider, database, store);

            registerTableManager(HugeType.VERTEX,
                    new ClickhouseTables.Vertex(store));

            registerTableManager(HugeType.EDGE_OUT,
                    ClickhouseTables.Edge.out(store));
            registerTableManager(HugeType.EDGE_IN,
                    ClickhouseTables.Edge.in(store));

            registerTableManager(HugeType.SECONDARY_INDEX,
                    new ClickhouseTables.SecondaryIndex(store));
            registerTableManager(HugeType.RANGE_INT_INDEX,
                    new ClickhouseTables.RangeIntIndex(store));
            registerTableManager(HugeType.RANGE_FLOAT_INDEX,
                    new ClickhouseTables.RangeFloatIndex(store));
            registerTableManager(HugeType.RANGE_LONG_INDEX,
                    new ClickhouseTables.RangeLongIndex(store));
            registerTableManager(HugeType.RANGE_DOUBLE_INDEX,
                    new ClickhouseTables.RangeDoubleIndex(store));
            registerTableManager(HugeType.SEARCH_INDEX,
                    new ClickhouseTables.SearchIndex(store));
            registerTableManager(HugeType.SHARD_INDEX,
                    new ClickhouseTables.ShardIndex(store));
            registerTableManager(HugeType.UNIQUE_INDEX,
                    new ClickhouseTables.UniqueIndex(store));
        }

        @Override
        public boolean isSchemaStore() {
            return false;
        }

        @Override
        public Id nextId(HugeType type) {
            throw new UnsupportedOperationException("MysqlGraphStore.nextId()");
        }

        @Override
        public void increaseCounter(HugeType type, long num) {
            throw new UnsupportedOperationException(
                    "MysqlGraphStore.increaseCounter()");
        }

        @Override
        public long getCounter(HugeType type) {
            throw new UnsupportedOperationException(
                    "MysqlGraphStore.getCounter()");
        }
    }
}
