package com.mininglamp.hugegraph.backend.store.clickhouse;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.AbstractBackendStore;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.exception.ConnectionException;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Map;
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
            // This exception must be throwed because the db has not created
            if (!e.getMessage().startsWith("Unknown database") &&
                    !e.getMessage().endsWith("does not exist")) {
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
    }
}
