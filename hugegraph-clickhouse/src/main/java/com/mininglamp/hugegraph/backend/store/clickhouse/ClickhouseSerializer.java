package com.mininglamp.hugegraph.backend.store.clickhouse;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.serializer.TableBackendEntry;
import com.baidu.hugegraph.backend.serializer.TableSerializer;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.JsonUtil;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ClickhouseSerializer extends TableSerializer {

    @Override
    public ClickhouseBackendEntry newBackendEntry(HugeType type, Id id) {
        return new ClickhouseBackendEntry(type, id);
    }

    @Override
    protected TableBackendEntry newBackendEntry(TableBackendEntry.Row row) {
        return new ClickhouseBackendEntry(row);
    }

    @Override
    protected ClickhouseBackendEntry convertEntry(BackendEntry backendEntry) {
        if (!(backendEntry instanceof ClickhouseBackendEntry)) {
            throw new BackendException("Not support by ClickhouseSerialiser");
        }
        return (ClickhouseBackendEntry) backendEntry;
    }

    @Override
    protected Set<Object> parseIndexElemIds(TableBackendEntry entry) {
        Set<Object> elemIds = InsertionOrderUtil.newSet();
        elemIds.add(entry.column(HugeKeys.ELEMENT_IDS));
        entry.subRows().forEach(row -> elemIds.add(row.column(HugeKeys.ELEMENT_IDS)));
        return elemIds;
    }

    @Override
    protected Id toId(Number number) {
        return IdGenerator.of(number.longValue());
    }

    @Override
    protected Id[] toIdArray(Object object) {
        assert object instanceof String;
        String value = (String) object;
        Number[] values = JsonUtil.fromJson(value, Number[].class);
        Id[] ids = new Id[values.length];
        int i = 0;
        for (Number number : values) {
            ids[i++] = IdGenerator.of(number.longValue());
        }
        return ids;
    }

    @Override
    protected Object toLongSet(Collection<Id> ids) {
        return this.toLongList(ids);
    }

    @Override
    protected Object toLongList(Collection<Id> ids) {
        long[] values = new long[ids.size()];
        int i = 0;
        for (Id id : ids) {
            values[i++] = id.asLong();
        }
        return JsonUtil.toJson(values);
    }

    // TODO implementation should be confirmed later
    @Override
    protected void formatProperty(HugeProperty<?> prop,
                                  TableBackendEntry.Row row) {
        throw new BackendException("Not support updating single property " +
                "by Clickhouse");
    }

    @Override
    protected void formatProperties(HugeElement element,
                                    TableBackendEntry.Row row) {
        Map<Number, Object> properties = new HashMap<>();
        // Add all properties of a Vertex
        for (HugeProperty<?> prop : element.getProperties().values()) {
            Number key = prop.propertyKey().id().asLong();
            Object val = prop.value();
            properties.put(key, val);
        }
        row.column(HugeKeys.PROPERTIES, JsonUtil.toJson(properties));
    }

    @Override
    protected void parseProperties(HugeElement element,
                                   TableBackendEntry.Row row) {
        String properties = row.column(HugeKeys.PROPERTIES);
        // Query edge will wrapped by a vertex, whose properties is empty
        if (properties.isEmpty()) {
            return;
        }

        Map<String, Object> props = JsonUtil.fromJson(properties, Map.class);
        for (Map.Entry<String, Object> prop : props.entrySet()) {
            /*
             * The key is string instead of int, because the key in json
             * must be string
             */
            Id pkeyId = this.toId(Long.valueOf(prop.getKey()));
            String colJson = JsonUtil.toJson(prop.getValue());
            // Add property to the HugeElement passed in
            this.parseProperty(pkeyId, colJson, element);
        }
    }

    @Override
    protected void writeUserdata(SchemaElement schema,
                                 TableBackendEntry entry) {
        assert entry instanceof ClickhouseBackendEntry;
        entry.column(HugeKeys.USER_DATA, JsonUtil.toJson(schema.userdata()));
    }

    /**
     * Read the userdata from an entry, and set it into schema.
     * @param schema
     * @param entry
     */
    @Override
    protected void readUserdata(SchemaElement schema,
                                TableBackendEntry entry) {
        assert entry instanceof ClickhouseBackendEntry;
        // Parse all user data of a schema element
        String json = entry.column(HugeKeys.USER_DATA);
        Map<String, Object> userdata = JsonUtil.fromJson(json, Map.class);
        for (Map.Entry<String, Object> e : userdata.entrySet()) {
            schema.userdata(e.getKey(), e.getValue());
        }
    }


}
