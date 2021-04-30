package com.mininglamp.hugegraph.backend.store.clickhouse;

import com.baidu.hugegraph.backend.store.AbstractBackendStoreProvider;
import com.baidu.hugegraph.backend.store.BackendStore;

public class ClickhouseStoreProvider extends AbstractBackendStoreProvider {

    // Database name is the graphe name
    protected String database() {
        return this.graph().toLowerCase();
    }

    @Override
    protected BackendStore newSchemaStore(String store) {
        return new ClickhouseStore.ClickhouseSchemaStore(this, this.database(), store);
    }
}
