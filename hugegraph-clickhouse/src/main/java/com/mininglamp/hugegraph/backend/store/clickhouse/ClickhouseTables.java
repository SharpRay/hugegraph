package com.mininglamp.hugegraph.backend.store.clickhouse;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.TableDefine;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.HugeKeys;

import com.mininglamp.hugegraph.backend.store.clickhouse.ClickhouseSessions.Session;

import java.sql.ResultSet;
import java.sql.SQLException;
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

    public static class ClickhouseTemplate extends ClickhouseTable {

        protected TableDefine define;

        public ClickhouseTemplate(String table) {
            super(table);
        }

        @Override
        public TableDefine tableDefine() {
            return this.define;
        }
    }

    public static class Counters extends ClickhouseTemplate {

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
            )

            try {
                session.execute(insert, this.table());
            } catch (SQLException e) {
                throw new BackendException("Failed to update counters " + "with '%s'", insert);
            }
        }
    }

    public static class VertexLabel extends ClickhouseTemplate {

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
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class EdgeLabel extends ClickhouseTemplate {

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
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class PropertyKey extends ClickhouseTemplate {

        // property key table name's 'pk'
        public static final String TABLE = HugeType.PROPERTY_KEY.string();

        public PropertyKey() {
            super(TABLE);

            this.define = new TableDefine();
            // 'id' column of table 'pk'
            this.define.column(HugeKeys.ID, DATATYPE_PK);
            // 'name' column of table 'pk'
            this.define.column(HugeKeys.NAME, SMALL_TEXT);
            // 'base_type' column of table 'pk'
            this.define.column(HugeKeys.BASE_TYPE, TINYINT);
            // 'base_value' colume of table 'pk'
            this.define.column(HugeKeys.BASE_VALUE, DATATYPE_SL);
            // 'index_type' column of table 'pk'
            this.define.column(HugeKeys.INDEX_TYPE, TINYINT);
            // 'fields' column of table 'pk'
            this.define.column(HugeKeys.FIELDS, SMALL_JSON);
            // 'user_data' column of table 'pk' with JSON literal value
            this.define.column(HugeKeys.USER_DATA, LARGE_JSON);
            // 'status' column of table 'pk'
            this.define.column(HugeKeys.STATUS, TINYINT);
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class IndexLabel extends ClickhouseTemplate {

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
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class Vertex extends ClickhouseTemplate {

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
            this.define.keys(HugeKeys.ID);
        }
    }

    /**
     * Ub consideration of performance, Edge table should be
     * created with the partitioning by label,
     * so we could delete all the edges bind to a label
     * using 'DROP PARTITION' operation.
     */
    public static class Edge extends ClickhouseTemplate {

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
                    this.table(), formatKey(HugeKeys.LABEL));

            this.define = new TableDefine();
            this.define.column(HugeKeys.OWNER_VERTEX, SMALL_TEXT);
            this.define.column(HugeKeys.DIRECTION, TINYINT);
            this.define.column(HugeKeys.LABEL, DATATYPE_SL);
            this.define.column(HugeKeys.SORT_VALUES, SMALL_TEXT);
            this.define.column(HugeKeys.OTHER_VERTEX, SMALL_TEXT);
            this.define.column(HugeKeys.PROPERTIES, LARGE_JSON);
            this.define.column(HugeKeys.EXPIRED_TIME, BIGINT);
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
            }
        }

        public static String table(Directions direction) {
            assert direction == Directions.IN || direction == Directions.OUT;
            return direction.type().string() + TABLE_SUFFIX;
        }
    }
}
