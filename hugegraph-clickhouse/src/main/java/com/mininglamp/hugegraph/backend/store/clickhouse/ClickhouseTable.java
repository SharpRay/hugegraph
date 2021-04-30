package com.mininglamp.hugegraph.backend.store.clickhouse;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.BackendTable;
import com.baidu.hugegraph.backend.store.Shard;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.google.common.collect.ImmutableList;
import com.mininglamp.hugegraph.backend.store.clickhouse.ClickhouseSessions.Session;
import com.baidu.hugegraph.util.Log;
import org.apache.commons.compress.utils.Lists;
import org.slf4j.Logger;

import java.util.List;

public class ClickhouseTable
        extends BackendTable<Session, ClickhouseBackendEntry.Row> {

    private static final Logger LOG = Log.logger(ClickhouseTable.class);

    private static final String DECIMAL = "Decimal";

    // The template for insert and delete statements
    private String insertTemplate;
    private String insertTemplateTtl;
    private String deleteTemplate;

    private final ClickhouseShardSpliter shardSpliter;

    public ClickhouseTable(String table) {
        super(table);
        this.insertTemplate = null;
        this.insertTemplateTtl = null;
        this.deleteTemplate = null;
        this.shardSpliter = new ClickhouseShardSpliter(this.table());
    }

    protected List<Object> idColumnValue(Id id) {
        return ImmutableList.of(id.asObject());
    }

    protected static String formatKey(HugeKeys key) {
        return key.name();
    }


    private static class ClickhouseShardSpliter extends ShardSpliter<Session> {

        private static final String BASE64 =
                "0123456789=?ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                        "abcdefghijklmnopqrstuvwxyz";
        private static final int COUNT = 64;

        public ClickhouseShardSpliter(String table) {
            super(table);
        }

        @Override
        public List<Shard> getSplits(Session session, long splitSize) {
            return Lists.newArrayList();
        }

        @Override
        protected long estimateDataSize(Session session) {
            return 0L;
        }

        @Override
        protected long estimateNumKeys(Session session) {
            return 0L;
        }
    }
}
