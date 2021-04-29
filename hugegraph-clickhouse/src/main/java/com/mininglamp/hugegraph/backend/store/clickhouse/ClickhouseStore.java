package com.mininglamp.hugegraph.backend.store.clickhouse;

import com.alipay.sofa.jraft.storage.snapshot.remote.Session;
import com.baidu.hugegraph.backend.store.AbstractBackendStore;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClickhouseStore extends AbstractBackendStore<Session> {

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

    public static class ClickhouseSchemaStore extends ClickhouseStore {

        private final ClickhouseTable
    }
}
