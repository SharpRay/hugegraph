package com.baidu.hugegraph.backend.store.clickhouse;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.*;
import com.baidu.hugegraph.backend.serializer.TableBackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntryIterator;
import com.baidu.hugegraph.backend.store.TableDefine;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.HugeKeys;

import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.backend.store.clickhouse.ClickhouseSessions.Session;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ClickhouseTables {

    public static final String BOOLEAN = "UInt8";
    public static final String TINYINT = "Int8";
    public static final String INT = "Int32";
    public static final String BIGINT = "Int64";
    public static final String NUMERIC = "Float64";
    public static final String SMALL_TEXT = "String";
    public static final String MID_TEXT = "String";
    public static final String LARGE_TEXT = "String";
    public static final String HUGE_TEXT = "String";

    private static final String DATATYPE_PK = INT;
    private static final String DATATYPE_SL = INT;  // VL/EL
    private static final String DATATYPE_IL = INT;

    private static final String SMALL_JSON = MID_TEXT;
    private static final String LARGE_JSON = LARGE_TEXT;

    public static class ClickhouseTableTemplate extends ClickhouseTable {

        protected TableDefine define;

        public ClickhouseTableTemplate(String table) {
            super(table);
        }

        @Override
        public TableDefine tableDefine() {
            return this.define;
        }
    }

    public static class Counters extends ClickhouseTableTemplate {

        // counter table name's 'c'
        public static final String TABLE = HugeType.COUNTER.string();

        public Counters() {
            super(TABLE);

            this.define = new TableDefine();
            // One of counter table's column name's 'schema_type'
            this.define.column(HugeKeys.SCHEMA_TYPE, SMALL_TEXT);
            // One of counter table's column name's 'id'
            this.define.column(HugeKeys.ID, INT);
            this.define.keys(HugeKeys.SCHEMA_TYPE);
        }

        public long getCounter(Session session, HugeType type) {
            String schemaCol = formatKey(HugeKeys.SCHEMA_TYPE);
            String idCol = formatKey(HugeKeys.ID);

            // Get id from counter table using schema_type field with value type
            String select = String.format("SELECT MAX(id) AS ID FROM %s WHERE %s = '%s';",
                    this.table(), schemaCol, type.name());

            try {
                ResultSet resultSet = session.select(select);
                if (resultSet.next()) {
                    return resultSet.getLong(idCol);
                } else {
                    return 0L;
                }
            } catch (SQLException e) {
                throw new BackendException(
                        "Failed to get id from counters with type '%s'",
                        e, type);
            }
        }

        public void increaseCounter(Session session,
                                    HugeType type, long increment) {
            String schemaCol = formatKey(HugeKeys.SCHEMA_TYPE);

            String insert = String.format(
                    "INSERT INTO %s SELECT " +
                            "(SELECT MAX(id) + %s FROM %s WHERE %s = '%s') AS id, " +
                            "'%s' AS %s",
                    this.table(), increment, this.table(),
                    schemaCol, type.name(), type.name(), schemaCol);

            try {
                session.execute(insert, this.table());
            } catch (SQLException e) {
                throw new BackendException("Failed to update counters " + "with '%s'", insert);
            }
        }
    }

    public static class VertexLabel extends ClickhouseTableTemplate {

        // vertex label table name's 'vl'
        public static final String TABLE = HugeType.VERTEX_LABEL.string();

        public VertexLabel() {
            super(TABLE);

            this.define = new TableDefine();
            // 'id' column of table 'vl'
            this.define.column(HugeKeys.ID, DATATYPE_SL);
            // 'name' column of table 'vl'
            this.define.column(HugeKeys.NAME, SMALL_TEXT);
            // 'id_strategy' column of table 'vl'
            this.define.column(HugeKeys.ID_STRATEGY, TINYINT);
            // 'primary_keys' column of table 'vl' with JSON literal value
            this.define.column(HugeKeys.PRIMARY_KEYS, SMALL_JSON);
            // 'properties' column of table 'vl' with JSON literal value
            this.define.column(HugeKeys.PROPERTIES, SMALL_JSON);
            // 'nullable_keys' column of table 'vl' with JSON literal value
            this.define.column(HugeKeys.NULLABLE_KEYS, SMALL_JSON);
            // 'index_labels' column of table 'vl' with JSON literal value
            this.define.column(HugeKeys.INDEX_LABELS, SMALL_JSON);
            // 'enable_label_index' column of table 'vl'
            this.define.column(HugeKeys.ENABLE_LABEL_INDEX, BOOLEAN);
            // 'user_data' column of table 'vl' with JSON literal value
            this.define.column(HugeKeys.USER_DATA, LARGE_JSON);
            // 'status' column of table 'vl'
            this.define.column(HugeKeys.STATUS, TINYINT);
            // 'ttl' column of table 'vl'
            this.define.column(HugeKeys.TTL, INT);
            // 'ttl_start_time' column of table 'vl'
            this.define.column(HugeKeys.TTL_START_TIME, DATATYPE_PK);
            // The column indicates that the record was deleted or not
            this.define.column(HugeKeys.DELETED, BOOLEAN);
            // The column as the version field of ReplacingMergeTree
            this.define.column(HugeKeys.UPDATE_NANO, BIGINT);
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class EdgeLabel extends ClickhouseTableTemplate {

        // edge label table name's 'el'
        public static final String TABLE = HugeType.EDGE_LABEL.string();

        public EdgeLabel() {
            super(TABLE);

            this.define = new TableDefine();
            // 'id' column of table 'el'
            this.define.column(HugeKeys.ID, DATATYPE_SL);
            // 'name' column of table 'el'
            this.define.column(HugeKeys.NAME, SMALL_TEXT);
            // 'frequency' column of table 'el'
            this.define.column(HugeKeys.FREQUENCY, TINYINT);
            // 'source_label' column of table 'el'
            this.define.column(HugeKeys.SOURCE_LABEL, DATATYPE_SL);
            // 'target_label' column of table 'el'
            this.define.column(HugeKeys.TARGET_LABEL, DATATYPE_SL);
            // 'sort_keys' column of table 'el' with JSON literal value
            this.define.column(HugeKeys.SORT_KEYS, SMALL_JSON);
            // 'properties' column of table 'el' with JSON literal value
            this.define.column(HugeKeys.PROPERTIES, SMALL_JSON);
            // 'nullable_keys' column of table 'el' with JSON literal value
            this.define.column(HugeKeys.NULLABLE_KEYS, SMALL_JSON);
            // 'index_labels' column of table 'el' with JSON literal value
            this.define.column(HugeKeys.INDEX_LABELS, SMALL_JSON);
            // 'enable_label_index' column of table 'el'
            this.define.column(HugeKeys.ENABLE_LABEL_INDEX, BOOLEAN);
            // 'user_data' column of table 'el' with JSON literal value
            this.define.column(HugeKeys.USER_DATA, LARGE_JSON);
            // 'status' column of table 'el'
            this.define.column(HugeKeys.STATUS, TINYINT);
            // 'ttl' column of table 'el'
            this.define.column(HugeKeys.TTL, INT);
            // 'ttl_start_time' column of table 'el'
            this.define.column(HugeKeys.TTL_START_TIME, DATATYPE_PK);
            // The column indicates that the record was deleted or not
            this.define.column(HugeKeys.DELETED, BOOLEAN);
            // The column as the version field of ReplacingMergeTree
            this.define.column(HugeKeys.UPDATE_NANO, BIGINT);
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class PropertyKey extends ClickhouseTableTemplate {

        // property key table name's 'pk'
        public static final String TABLE = HugeType.PROPERTY_KEY.string();

        public PropertyKey() {
            super(TABLE);

            this.define = new TableDefine();
            // 'id' column of table 'pk'
            this.define.column(HugeKeys.ID, DATATYPE_PK);
            // 'name' column of table 'pk'
            this.define.column(HugeKeys.NAME, SMALL_TEXT);
            // 'data_type' column of table 'pk'
            this.define.column(HugeKeys.DATA_TYPE, TINYINT);
            // 'cardinality' colume of table 'pk'
            this.define.column(HugeKeys.CARDINALITY, TINYINT);
            // 'aggregate_type' column of table 'pk'
            this.define.column(HugeKeys.AGGREGATE_TYPE, TINYINT);
            // 'properties' column of table 'pk'
            this.define.column(HugeKeys.PROPERTIES, SMALL_JSON);
            // 'user_data' column of table 'pk' with JSON literal value
            this.define.column(HugeKeys.USER_DATA, LARGE_JSON);
            // 'status' column of table 'pk'
            this.define.column(HugeKeys.STATUS, TINYINT);
            // The column indicates that the record was deleted or not
            this.define.column(HugeKeys.DELETED, BOOLEAN);
            // The column as the version field of ReplacingMergeTree
            this.define.column(HugeKeys.UPDATE_NANO, BIGINT);
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class IndexLabel extends ClickhouseTableTemplate {

        // index label table name's 'IL'
        public static final String TABLE = HugeType.INDEX_LABEL.string();

        public IndexLabel() {
            super(TABLE);

            this.define = new TableDefine();
            // 'id' column of table 'il'
            this.define.column(HugeKeys.ID, DATATYPE_IL);
            // 'name' column of table 'il'
            this.define.column(HugeKeys.NAME, SMALL_TEXT);
            // 'base_type' column of table 'il'
            this.define.column(HugeKeys.BASE_TYPE, TINYINT);
            // 'base_value' column of table 'il'
            this.define.column(HugeKeys.BASE_VALUE,DATATYPE_SL);
            // 'index_type' column of table 'il'
            this.define.column(HugeKeys.INDEX_TYPE, TINYINT);
            // 'fields' column of table 'il'
            this.define.column(HugeKeys.FIELDS, SMALL_JSON);
            // 'user_data' column of table 'il' with JSON literal value
            this.define.column(HugeKeys.USER_DATA, LARGE_JSON);
            // 'status' column of table 'il'
            this.define.column(HugeKeys.STATUS, TINYINT);
            // The column indicates that the record was deleted or not
            this.define.column(HugeKeys.DELETED, BOOLEAN);
            // The column as the version field of ReplacingMergeTree
            this.define.column(HugeKeys.UPDATE_NANO, BIGINT);
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class Vertex extends ClickhouseTableTemplate {

        // vertex table name's '${store}_v'
        public static final String TABLE = HugeType.VERTEX.string();

        public Vertex(String store) {
            super(joinTableName(store, TABLE));

            this.define = new TableDefine();
            // 'id' column of table 'V'
            this.define.column(HugeKeys.ID, SMALL_TEXT);
            // 'label' column of table 'V'
            this.define.column(HugeKeys.LABEL, DATATYPE_SL);
            // 'properties' column of table 'V'
            this.define.column(HugeKeys.PROPERTIES, HUGE_TEXT);
            // 'expired_time' column of table 'V'
            this.define.column(HugeKeys.EXPIRED_TIME, BIGINT);
            // The column indicates that the record was deleted or not
            this.define.column(HugeKeys.DELETED, BOOLEAN);
            // The column as the version field of ReplacingMergeTree
            this.define.column(HugeKeys.UPDATE_NANO, BIGINT);
            this.define.keys(HugeKeys.ID);
        }
    }

    /**
     * In consideration of performance, Edge table should be
     * created with the partitioning by label,
     * so we could delete all the edges bind to a label
     * using 'DROP PARTITION' operation.
     */
    public static class Edge extends ClickhouseTableTemplate {

        /*
         * edge table with the suffix 'e',
         * According to the direction(IN|OUT), the table name
         * should be '${store}_ie' or '${store}_oe'
         */
        public static final String TABLE_SUFFIX = HugeType.EDGE.string();

        private final Directions direction;
        private final String delByLabelTemplate;

        public Edge(String store, Directions direction) {
            super(joinTableName(store, table(direction)));

            this.direction = direction;
            this.delByLabelTemplate = String.format(
                    "ALTER TABLE %s DROP PART '?';",
                    this.table());

            this.define = new TableDefine();
            this.define.column(HugeKeys.OWNER_VERTEX, SMALL_TEXT);
            this.define.column(HugeKeys.DIRECTION, TINYINT);
            this.define.column(HugeKeys.LABEL, DATATYPE_SL);
            this.define.column(HugeKeys.SORT_VALUES, SMALL_TEXT);
            this.define.column(HugeKeys.OTHER_VERTEX, SMALL_TEXT);
            this.define.column(HugeKeys.PROPERTIES, LARGE_JSON);
            this.define.column(HugeKeys.EXPIRED_TIME, BIGINT);
            // The column indicates that the record was deleted or not
            this.define.column(HugeKeys.DELETED, BOOLEAN);
            // The column as the version field of ReplacingMergeTree
            this.define.column(HugeKeys.UPDATE_NANO, BIGINT);
            this.define.keys(HugeKeys.OWNER_VERTEX, HugeKeys.DIRECTION,
                    HugeKeys.LABEL, HugeKeys.SORT_VALUES,
                    HugeKeys.OTHER_VERTEX);
        }

        @Override
        public List<Object> idColumnValue(Id id) {
            EdgeId edgeId;
            if (id instanceof EdgeId) {
                edgeId = (EdgeId) id;
            } else {
                String[] idparts = EdgeId.split(id);
                if (idparts.length == 1) {
                    // Delete edge by label (The id is label id)
                    return Arrays.asList(idparts);
                }
                id = IdUtil.readString(id.asString());
                edgeId = EdgeId.parse(id.asString());
            }

            E.checkState(edgeId.direction() == this.direction,
                    "Can't query %s edges from %s edges table",
                    edgeId.direction(), this.direction);

            List<Object> list = new ArrayList<>(5);
            list.add(IdUtil.writeStoredString(edgeId.ownerVertexId()));
            list.add(edgeId.directionCode());
            list.add(edgeId.edgeLabelId().asLong());
            list.add(edgeId.sortValues());
            list.add(IdUtil.writeStoredString(edgeId.otherVertexId()));
            return list;
        }

        @Override
        public void delete(Session session, TableBackendEntry.Row entry) {
            // Let super class do delete if not deleting edge by label
            List<Object> idParts = this.idColumnValue(entry.id());
            if (idParts.size() > 1 || entry.columns().size() > 0)  {
                super.delete(session, entry);
                return;
            }

            // The only element is label
            this.deleteEdgesByLabel(session, entry.id());
        }

        private void deleteEdgesByLabel(Session session, Id label) {
            PreparedStatement deleteStmt;
            try {
                // Create or get delete prepare statement
                deleteStmt = session.prepareStatement(this.delByLabelTemplate);
                // Delete edges
                deleteStmt.setObject(1, label.asLong());
            } catch (SQLException e) {
                throw new BackendException("Failed to prepare statement '%s'",
                        this.delByLabelTemplate);
            }
            session.add(deleteStmt);
        }

        @Override
        public BackendEntry mergeEntries(BackendEntry e1, BackendEntry e2) {
            // Merge edges into vertex

            ClickhouseBackendEntry current = (ClickhouseBackendEntry) e1;
            ClickhouseBackendEntry next = (ClickhouseBackendEntry) e2;

            E.checkState(current == null || current.type().isVertex(),
                    "The current entry must be null or VERTEX");
            E.checkState(next != null && next.type().isEdge(),
                    "The next entry must be EDGE");

            long maxSize = BackendEntryIterator.INLINE_BATCH_SIZE;
            if (current != null && current.subRows().size() < maxSize) {
                Id nextVertexId = IdGenerator.of(next.<String>column(HugeKeys.OWNER_VERTEX));
                if (current.id().equals(nextVertexId)) {
                    current.subRow(next.row());
                    return current;
                }
            }

            return this.wrapByVertex(next);
        }

        private ClickhouseBackendEntry wrapByVertex(ClickhouseBackendEntry edge) {
            assert edge.type().isEdge();
            String ownerVertex = edge.column(HugeKeys.OWNER_VERTEX);
            E.checkState(ownerVertex != null, "Invalid backend entry");
            Id vertexId = IdGenerator.of(ownerVertex);
            ClickhouseBackendEntry vertex = new ClickhouseBackendEntry(HugeType.VERTEX, vertexId);

            vertex.column(HugeKeys.ID, ownerVertex);
            vertex.column(HugeKeys.PROPERTIES, "");

            vertex.subRow(edge.row());
            return vertex;
        }

        public static String table(Directions direction) {
            assert direction == Directions.OUT || direction == Directions.IN;
            return direction.type().string() + TABLE_SUFFIX;
        }

        public static ClickhouseTable out(String store) {
            return new Edge(store, Directions.OUT);
        }

        public static ClickhouseTable in(String store) {
            return new Edge(store, Directions.IN);
        }
    }

    public abstract static class Index extends ClickhouseTableTemplate {

        public Index(String table) {
            super(table);
        }

        public abstract String entryId(ClickhouseBackendEntry entry);
    }

    public static class SecondaryIndex extends Index {

        // secondary index table name's '${store}_si'
        public static final String TABLE = HugeType.SECONDARY_INDEX.string();

        public SecondaryIndex(String store) {
            this(store, TABLE);
        }

        public SecondaryIndex(String store, String table) {
            super(joinTableName(store, table));

            this.define = new TableDefine();
            this.define.column(HugeKeys.FIELD_VALUES, SMALL_TEXT);
            this.define.column(HugeKeys.INDEX_LABEL_ID, DATATYPE_IL);
            this.define.column(HugeKeys.ELEMENT_IDS, SMALL_TEXT);
            this.define.column(HugeKeys.EXPIRED_TIME, BIGINT);
            // The column indicates that the record was deleted or not
            this.define.column(HugeKeys.DELETED, BOOLEAN);
            // The column as the version field of ReplacingMergeTree
            this.define.column(HugeKeys.UPDATE_NANO, BIGINT);
            this.define.keys(HugeKeys.INDEX_LABEL_ID,
                    HugeKeys.FIELD_VALUES,
                    HugeKeys.ELEMENT_IDS);
        }

        @Override
        public final String entryId(ClickhouseBackendEntry entry) {
            String fieldValues = entry.column(HugeKeys.FIELD_VALUES);
            Integer labelId = entry.column(HugeKeys.INDEX_LABEL_ID);
            return SplicingIdGenerator.concat(fieldValues, labelId.toString());
        }
    }

    public static class SearchIndex extends SecondaryIndex {

        // search index table name's '${store}_ai'
        public static final String TABLE = HugeType.SEARCH_INDEX.string();

        public SearchIndex(String store) {
            super(store, TABLE);
        }
    }

    public static class UniqueIndex extends SecondaryIndex {

        // unique index table name's '${store}_ui'
        public static final String TABLE = HugeType.UNIQUE_INDEX.string();

        public UniqueIndex(String store) {
            super(store, TABLE);
        }
    }

    public static class RangeIndex extends Index {

        public RangeIndex(String store, String table) {
            super(joinTableName(store, table));

            this.define = new TableDefine();
            this.define.column(HugeKeys.INDEX_LABEL_ID, DATATYPE_IL);
            this.define.column(HugeKeys.FIELD_VALUES, NUMERIC);
            this.define.column(HugeKeys.ELEMENT_IDS, SMALL_TEXT);
            this.define.column(HugeKeys.EXPIRED_TIME, BIGINT);
            // The column indicates that the record was deleted or not
            this.define.column(HugeKeys.DELETED, BOOLEAN);
            // The column as the version field of ReplacingMergeTree
            this.define.column(HugeKeys.UPDATE_NANO, BIGINT);
            this.define.keys(HugeKeys.INDEX_LABEL_ID,
                    HugeKeys.FIELD_VALUES,
                    HugeKeys.ELEMENT_IDS);
        }

        @Override
        public final String entryId(ClickhouseBackendEntry entry) {
            Double fieldValues = entry.<Double>column(HugeKeys.FIELD_VALUES);
            Integer labelId = entry.column(HugeKeys.INDEX_LABEL_ID);
            return SplicingIdGenerator.concat(labelId.toString(),
                    fieldValues.toString());
        }
    }

    public static class RangeIntIndex extends RangeIndex {

        // range int index table name's ${store}_ii
        public static final String TABLE = HugeType.RANGE_INT_INDEX.string();

        public RangeIntIndex(String store) {
            this(store, TABLE);
        }

        public RangeIntIndex(String store, String table) {
            super(store, table);

            this.define = new TableDefine();
            this.define.column(HugeKeys.INDEX_LABEL_ID, DATATYPE_IL);
            this.define.column(HugeKeys.FIELD_VALUES, INT);
            this.define.column(HugeKeys.ELEMENT_IDS, SMALL_TEXT);
            this.define.column(HugeKeys.EXPIRED_TIME, BIGINT);
            // The column indicates that the record was deleted or not
            this.define.column(HugeKeys.DELETED, BOOLEAN);
            // The column as the version field of ReplacingMergeTree
            this.define.column(HugeKeys.UPDATE_NANO, BIGINT);
            this.define.keys(HugeKeys.INDEX_LABEL_ID,
                    HugeKeys.FIELD_VALUES,
                    HugeKeys.ELEMENT_IDS);
        }
    }

    public static class RangeFloatIndex extends RangeIndex {

        // range float index table name's ${store}_fi
        public static final String TABLE = HugeType.RANGE_FLOAT_INDEX.string();

        public RangeFloatIndex(String store) {
            this(store, TABLE);
        }

        public RangeFloatIndex(String store, String table) {
            super(store, table);

            this.define = new TableDefine();
            this.define.column(HugeKeys.INDEX_LABEL_ID, DATATYPE_IL);
            this.define.column(HugeKeys.FIELD_VALUES, NUMERIC);
            this.define.column(HugeKeys.ELEMENT_IDS, SMALL_TEXT);
            this.define.column(HugeKeys.EXPIRED_TIME, BIGINT);
            // The column indicates that the record was deleted or not
            this.define.column(HugeKeys.DELETED, BOOLEAN);
            // The column as the version field of ReplacingMergeTree
            this.define.column(HugeKeys.UPDATE_NANO, BIGINT);
            this.define.keys(HugeKeys.INDEX_LABEL_ID,
                    HugeKeys.FIELD_VALUES,
                    HugeKeys.ELEMENT_IDS);
        }
    }

    public static class RangeLongIndex extends RangeIndex {

        // range long index table name's ${store}_li
        public static final String TABLE = HugeType.RANGE_LONG_INDEX.string();

        public RangeLongIndex(String store) {
            this(store, TABLE);
        }

        public RangeLongIndex(String store, String table) {
            super(store, table);

            this.define = new TableDefine();
            this.define.column(HugeKeys.INDEX_LABEL_ID, DATATYPE_IL);
            this.define.column(HugeKeys.FIELD_VALUES, BIGINT);
            this.define.column(HugeKeys.ELEMENT_IDS, SMALL_TEXT);
            this.define.column(HugeKeys.EXPIRED_TIME, BIGINT);
            // The column indicates that the record was deleted or not
            this.define.column(HugeKeys.DELETED, BOOLEAN);
            // The column as the version field of ReplacingMergeTree
            this.define.column(HugeKeys.UPDATE_NANO, BIGINT);
            this.define.keys(HugeKeys.INDEX_LABEL_ID,
                    HugeKeys.FIELD_VALUES,
                    HugeKeys.ELEMENT_IDS);
        }
    }

    public static class RangeDoubleIndex extends RangeIndex {

        // range double index table name's ${store}_di
        public static final String TABLE = HugeType.RANGE_DOUBLE_INDEX.string();

        public RangeDoubleIndex(String store) {
            this(store, TABLE);
        }

        public RangeDoubleIndex(String store, String table) {
            super(store, table);

            this.define = new TableDefine();
            this.define.column(HugeKeys.INDEX_LABEL_ID, DATATYPE_IL);
            this.define.column(HugeKeys.FIELD_VALUES, NUMERIC);
            this.define.column(HugeKeys.ELEMENT_IDS, SMALL_TEXT);
            this.define.column(HugeKeys.EXPIRED_TIME, BIGINT);
            // The column indicates that the record was deleted or not
            this.define.column(HugeKeys.DELETED, BOOLEAN);
            // The column as the version field of ReplacingMergeTree
            this.define.column(HugeKeys.UPDATE_NANO, BIGINT);
            this.define.keys(HugeKeys.INDEX_LABEL_ID,
                    HugeKeys.FIELD_VALUES,
                    HugeKeys.ELEMENT_IDS);
        }
    }

    public static class ShardIndex extends Index {

        // shard index table name's ${store}_hi
        public static final String TABLE = HugeType.SHARD_INDEX.string();

        public ShardIndex(String store) {
            this(store, TABLE);
        }

        public ShardIndex(String store, String table) {
            super(joinTableName(store, table));

            this.define = new TableDefine();
            this.define.column(HugeKeys.INDEX_LABEL_ID, DATATYPE_IL);
            this.define.column(HugeKeys.FIELD_VALUES, SMALL_TEXT);
            this.define.column(HugeKeys.ELEMENT_IDS, SMALL_TEXT);
            this.define.column(HugeKeys.EXPIRED_TIME, BIGINT);
            // The column indicates that the record was deleted or not
            this.define.column(HugeKeys.DELETED, BOOLEAN);
            // The column as the version field of ReplacingMergeTree
            this.define.column(HugeKeys.UPDATE_NANO, BIGINT);
            this.define.keys(HugeKeys.INDEX_LABEL_ID,
                    HugeKeys.FIELD_VALUES,
                    HugeKeys.ELEMENT_IDS);
        }

        @Override
        public final String entryId(ClickhouseBackendEntry entry) {
            Double fieldValues = entry.<Double>column(HugeKeys.FIELD_VALUES);
            Integer labelId = entry.column(HugeKeys.INDEX_LABEL_ID);
            return SplicingIdGenerator.concat(labelId.toString(),
                    fieldValues.toString());
        }
    }
}
