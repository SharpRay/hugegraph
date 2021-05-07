package com.mininglamp.hugegraph.backend.store.clickhouse;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.serializer.TableBackendEntry;
import com.baidu.hugegraph.type.HugeType;

public class ClickhouseBackendEntry extends TableBackendEntry {

    public ClickhouseBackendEntry(Id id) {
        super(id);
    }

    public ClickhouseBackendEntry(HugeType type) {
        this(type, null);
    }

    public ClickhouseBackendEntry(HugeType type, Id id) {
        this(new Row(type, id));
    }

    public ClickhouseBackendEntry(TableBackendEntry.Row row) {
        super(row);
    }
}
