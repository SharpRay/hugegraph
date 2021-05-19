package com.baidu.hugegraph.backend.store.clickhouse;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.page.PageState;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntryIterator;
import com.baidu.hugegraph.backend.store.mysql.MysqlEntryIterator;
import com.baidu.hugegraph.backend.store.mysql.MysqlTable;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.StringEncoding;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class ClickhouseEntryIterator extends BackendEntryIterator {

    private final ResultSet results;
    private final BiFunction<BackendEntry, BackendEntry, BackendEntry> merger;

    private BackendEntry next;
    private BackendEntry lastest;
    private boolean exceedLimit;

    public ClickhouseEntryIterator(ResultSet rs, Query query,
                                   BiFunction<BackendEntry, BackendEntry, BackendEntry> merger) {
        super(query);
        this.results = rs;
        this.merger = merger;
        this.next = null;
        this.lastest = null;
        this.exceedLimit = false;
    }

    @Override
    protected final boolean fetch() {
        assert this.current == null;
        if (this.next != null) {
            this.current = this.next;
            this.next = null;
        }

        try {
            while (!this.results.isClosed() && this.results.next()) {
                ClickhouseBackendEntry entry = this.row2Entry(this.results);
                this.lastest = entry;
                BackendEntry merged = this.merger.apply(this.current, entry);
                if (this.current == null) {
                    // The first time to read
                    this.current = merged;
                } else if (merged == this.current) {
                    // Does the next entry belongs to the current entry
                    assert merged != null;
                } else {
                    // New entry
                    assert this.next == null;
                    this.next = merged;
                    break;
                }

                // When limit exceed, stop fetching
                if (this.reachLimit(this.fetched() - 1)) {
                    this.exceedLimit = true;
                    // Need to remove last one because fetched limit + 1 records
                    this.removeLastRecord();
                    this.results.close();
                    break;
                }
            }
        } catch (SQLException e) {
            throw new BackendException("Fetch next error", e);
        }
        return this.current != null;
    }

    @Override
    protected PageState pageState() {
        byte[] position;
        // There is no latest or no next page
        if (this.lastest == null || !this.exceedLimit &&
                this.fetched() <= this.query.limit() && this.next == null) {
            position = PageState.EMPTY_BYTES;
        } else {
            ClickhouseBackendEntry entry = (ClickhouseBackendEntry) this.lastest;
            position = new PagePosition(entry.columnsMap()).toBytes();
        }
        return new PageState(position, 0, (int) this.count());
    }

    @Override
    protected void skipOffset() {
        // pass
    }

    @Override
    protected final long sizeOf(BackendEntry entry) {
        ClickhouseBackendEntry e = (ClickhouseBackendEntry) entry;
        int subRowsSize = e.subRows().size();
        return subRowsSize > 0 ? subRowsSize : 1L;
    }

    @Override
    protected final long skip(BackendEntry entry, long skip) {
        ClickhouseBackendEntry e = (ClickhouseBackendEntry) entry;
        E.checkState(e.subRows().size() > skip, "Invalid entry to skip");
        for (long i = 0; i < skip; ++i) {
            e.subRows().remove(0);
        }
        return e.subRows().size();
    }

    @Override
    public void close() throws Exception {
        this.results.close();
    }

    private void removeLastRecord() {
        ClickhouseBackendEntry entry = (ClickhouseBackendEntry) this.current;
        int lastOne = entry.subRows().size() - 1;
        assert lastOne >= 0;
        entry.subRows().remove(lastOne);
    }

    /**
     * UInt8 is the boolean type in CH,
     * this transformation could avoid
     * the type casting error in deserialization.
     */
    private Object int2Boolean(HugeKeys key, Object value) {
        if (key != HugeKeys.ENABLE_LABEL_INDEX) {
            return value;
        }
        if ((Integer) value == 1) {
            return true;
        }
        return false;
    }

    private ClickhouseBackendEntry row2Entry(ResultSet result) throws SQLException {
        HugeType type = this.query.resultType();
        ClickhouseBackendEntry entry = new ClickhouseBackendEntry(type);
        ResultSetMetaData metaData = result.getMetaData();
        for (int i = 1; i <= metaData.getColumnCount(); ++i) {
            String name = metaData.getColumnLabel(i);
            HugeKeys key = ClickhouseTable.parseKey(name);
            Object value = int2Boolean(key, result.getObject(i));
            if (value == null) {
                assert key == HugeKeys.EXPIRED_TIME;
                continue;
            }
            entry.column(key, value);
        }
        return entry;
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
