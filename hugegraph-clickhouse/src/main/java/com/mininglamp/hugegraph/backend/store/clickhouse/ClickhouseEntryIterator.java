package com.mininglamp.hugegraph.backend.store.clickhouse;

import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntryIterator;
import com.baidu.hugegraph.backend.store.mysql.MysqlEntryIterator;
import com.baidu.hugegraph.backend.store.mysql.MysqlTable;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.StringEncoding;

import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class ClickhouseEntryIterator extends BackendEntryIterator {

    private final ResultSet results;
    private final BiFunction<BackendEntry, BackendEntry, BackendEntry> merger;

    private BackendEntry next;
    private BackendEntry lastest;
    private boolean exceeedLimit;

    public ClickhouseEntryIterator(ResultSet rs, Query query,
                                   BiFunction<BackendEntry, BackendEntry, BackendEntry> merger) {
        super(query);
        this.results = rs;
        this.merger = merger;
        this.next = null;
        this.lastest = null;
        this.exceeedLimit = false;
    }

    public static class PagePosition {

        private final Map<HugeKeys, Object> columns;

        public PagePosition(Map<HugeKeys, Object> columns) {
            this.columns = columns;
        }

        public Map<HugeKeys, Object> columns() {
            return this.columns;
        }

        @Override
        public String toString() {
            return JsonUtil.toJson(this.columns);
        }

        public byte[] toBytes() {
            String json = JsonUtil.toJson(this.columns);
            return StringEncoding.encode(json);
        }

        public static MysqlEntryIterator.PagePosition fromBytes(byte[] bytes) {
            String json = StringEncoding.decode(bytes);
            @SuppressWarnings("unchecked")
            Map<String, Object> columns = JsonUtil.fromJson(json, Map.class);
            Map<HugeKeys, Object> keyColumns = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : columns.entrySet()) {
                HugeKeys key = MysqlTable.parseKey(entry.getKey());
                keyColumns.put(key, entry.getValue());
            }
            return new MysqlEntryIterator.PagePosition(keyColumns);
        }
    }
}
