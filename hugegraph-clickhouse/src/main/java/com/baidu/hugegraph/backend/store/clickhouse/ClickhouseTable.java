package com.baidu.hugegraph.backend.store.clickhouse;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.page.PageState;
import com.baidu.hugegraph.backend.query.Aggregate;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.TableBackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendTable;
import com.baidu.hugegraph.backend.store.Shard;
import com.baidu.hugegraph.backend.store.TableDefine;
import com.baidu.hugegraph.backend.store.mysql.WhereBuilder;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.baidu.hugegraph.backend.store.clickhouse.ClickhouseSessions.Session;
import com.baidu.hugegraph.backend.store.clickhouse.ClickhouseEntryIterator.PagePosition;
import com.baidu.hugegraph.util.Log;
import org.apache.logging.log4j.util.Strings;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.baidu.hugegraph.backend.store.mysql.MysqlTable.formatKeys;

public abstract class ClickhouseTable
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

    @Override
    protected void registerMetaHandlers() {
        this.registerMetaHandler("splits", (session, meta, args) -> {
            E.checkArgument(args.length == 1,
                    "The args count of %s must be 1", meta);
            long splitSize = (long) args[0];
            return this.shardSpliter.getSplits(session, splitSize);
        });
    }

    public abstract TableDefine tableDefine();

    @Override
    public void init(Session session) {
        createTable(session, this.tableDefine());
    }

    @Override
    public void clear(Session session) {
        this.dropTable(session);
    }

    public void truncate(Session session) {
        this.truncateTable(session);
    }

    protected void createTable(Session session, TableDefine tableDefine) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ");
        sql.append(this.table()).append(" (");
        // Add column
        tableDefine.columns().entrySet().forEach(entry -> {
            sql.append(formatKey(entry.getKey()));
            sql.append(" ");
            sql.append(entry.getValue());
            sql.append(defaultValue(entry.getKey()));
            sql.append(",\n");
        });
        sql.append(")");
        // Specify engine
        sql.append(engine());
        // Specify partition
        sql.append(partition(tableDefine));
        // Specified primary keys
        sql.append(primaryKey(tableDefine));
        sql.append(";");

        LOG.debug("Create table: {}", sql);
        try {
            session.execute(sql.toString());
        } catch (SQLException e) {
            throw new BackendException("Failed to create table with '%s'",
                    e, sql);
        }
    }

    protected void dropTable(Session session) {
        LOG.debug("Drop table: {}", this.table());
        String sql = this.buildDropTemplate();
        try {
            session.execute(sql);
        } catch (SQLException e) {
            throw new BackendException("Failed to drop table with '%s'",
                    e, sql);
        }
    }

    protected void truncateTable(Session session) {
        LOG.debug("Truncate table: {}", this.table());
        String sql = this.buildTruncateTemplate();
        try {
            session.execute(sql);
        } catch (SQLException e) {
            throw new BackendException("Failed to truncate table with '%s'",
                e, sql);
        }
    }

    private StringBuilder defaultValue(HugeKeys key) {
        StringBuilder dv = new StringBuilder();
        if (key.equals(HugeKeys.UPDATE_NANO)) {
            dv.append(" DEFAULT toFloat64(now64(9)) * 1000000000");
        }
        if (key.equals(HugeKeys.DELETED)) {
            dv.append(" DEFAULT 0");
        }
        return dv;
    }

    private StringBuilder primaryKey(TableDefine tableDefine) {
        StringBuilder primaryKey = new StringBuilder();
        primaryKey.append("ORDER BY ");
        primaryKey.append(
            tableDefine.keys().stream()
                    .map(ClickhouseTable::formatKey)
                    .collect(Collectors.joining(","))
        );
        return primaryKey;
    }

    private StringBuilder engine() {
        StringBuilder enginePart = new StringBuilder();
        enginePart.append(String.format(" ENGINE=ReplacingMergeTree(%s)\n",
                HugeKeys.UPDATE_NANO.string()));
        return enginePart;
    }

    private StringBuilder partition(TableDefine tableDefine) {
        StringBuilder partitionPart = new StringBuilder();
        if (tableDefine.columns().containsKey(HugeKeys.LABEL)) {
            partitionPart.append(String.format(" PARTITION BY %s\n", HugeKeys.LABEL.string()));
        }
        return partitionPart;
    }

    @Override
    public void insert(Session session, ClickhouseBackendEntry.Row entry) {
        String template = this.buildInsertTemplate(entry);

        PreparedStatement insertStmt;
        try {
            insertStmt = session.prepareStatement(template);
            int i = 1;
            for (Object object : this.buildInsertObjects(entry)) {
                insertStmt.setObject(i++, object);
            }
        } catch (SQLException e) {
            throw new BackendException("Failed to prepare statement '%s' " +
                    "for entry %s", template, entry);
        }
        session.add(insertStmt);
    }

    protected List<Object> buildInsertObjects(ClickhouseBackendEntry.Row entry) {
        List<Object> objects = new ArrayList<>();
        entry.columns().entrySet().forEach(e -> {
            Object value = e.getValue();
            String type = this.tableDefine().columns().get(e.getKey());
            if (type.startsWith(DECIMAL)) {
                value = new BigDecimal(value.toString());
            }
            objects.add(value);
        });
        return objects;
    }

    protected String buildInsertTemplate(ClickhouseBackendEntry.Row entry) {
        if (entry.ttl() != 0L) {
            return this.buildInsertTemplateWithTtl(entry);
        }
        if (this.insertTemplate != null) {
            return this.insertTemplate;
        }

        this.insertTemplate = this.buildInsertTemplateForce(entry);
        return this.insertTemplate;
    }

    protected String buildInsertTemplateWithTtl(ClickhouseBackendEntry.Row entry) {
        assert entry.ttl() != 0L;
        if (this.insertTemplateTtl != null) {
            return this.insertTemplateTtl;
        }

        this.insertTemplateTtl = this.buildInsertTemplateForce(entry);
        return this.insertTemplateTtl;
    }

    protected String buildInsertTemplateForce(ClickhouseBackendEntry.Row entry) {
        StringBuilder insert = new StringBuilder();
        insert.append("INSERT INTO ").append(this.table()).append(" (");

        int i = 0;
        int n = entry.columns().size();
        for (HugeKeys key : entry.columns().keySet()) {
            insert.append(formatKey(key));
            if (++i != n) {
                insert.append(", ");
            }
        }
        insert.append(") VALUES (");
        // Fill with '?'
        for (i = 0; i < n; i++) {
            insert.append("?");
            if (i != n - 1) {
                insert.append(", ");
            }
        }
        insert.append(")");

        return insert.toString();
    }

    protected String buildDropTemplate() {
        return String.format("DROP TABLE IF EXISTS %s;", this.table());
    }

    protected String buildTruncateTemplate() {
        return String.format("TRUNCATE TABLE %s;", this.table());
    }

    @Override
    public void delete(Session session, TableBackendEntry.Row entry) {
        List<HugeKeys> idNames = this.idColumnName();
        String template = this.buildDeleteTemplate(idNames);
        PreparedStatement stmt;
        try {
            stmt = session.prepareStatement(template);
            if (entry.columns().isEmpty()) {
                // Delete just by id
                List<Long> idValues = this.idColumnValue(entry);
                assert idNames.size() == idValues.size();

                for (int i = 0, n = idNames.size(); i < n; ++i) {
                    stmt.setObject(i + 1, idValues.get(i));
                }
            } else {
                // Delete just by column keys (must be id columns)
                for (int i = 0, n = idNames.size(); i < n; ++i) {
                    HugeKeys key = idNames.get(i);
                    Object value = entry.column(key);

                    stmt.setObject(i + 1, value);
                }
            }
        } catch (SQLException e) {
            throw new BackendException("Failed to prepare statement '%s' " +
                    "with entry columns %s",
                    template, entry.columns().values());
        }
        session.add(stmt);
    }

    @Override
    public void append(Session session, ClickhouseBackendEntry.Row entry) {
        this.insert(session, entry);
    }

    @Override
    public void eliminate(Session session, ClickhouseBackendEntry.Row entry) {
        this.delete(session, entry);
    }

    @Override
    public Number queryNumber(Session session, Query query) {
        Aggregate aggr = query.aggregateNotNull();

        Iterator<Number> results = this.query(session, query, (q, rs) -> {
            try {
                if (!rs.next()) {
                    return IteratorUtils.of(aggr.defaultValue());
                }
                return IteratorUtils.of(rs.getLong(1));
            } catch (SQLException e) {
                throw new BackendException(e);
            }
        });
        return aggr.reduce(results);
    }

    public static HugeKeys parseKey(String name) {
        return HugeKeys.valueOf(name.toUpperCase());
    }

    @Override
    public Iterator<BackendEntry> query(Session session, Query query) {
        return this.query(session, query, this::results2Entries);
    }

    protected Iterator<BackendEntry> results2Entries(Query query, ResultSet results) {
        return new ClickhouseEntryIterator(results, query, this::mergeEntries);
    }

    protected <R> Iterator<R> query(Session session, Query query,
                                    BiFunction<Query, ResultSet,
                                            Iterator<R>> parser) {
        ExtendableIterator<R> rs = new ExtendableIterator<>();

        if (query.limit() == 0L && !query.noLimit()) {
            LOG.debug("Return empty result(limit=0) for query {}", query);
            return rs;
        }

        List<StringBuilder> selections = this.query2Select(this.table(), query);
        try {
            for (StringBuilder selection : selections) {
                ResultSet results = session.select(selection.toString());
                rs.extend(parser.apply(query, results));
            }
        } catch (SQLException e) {
            throw new BackendException("Failed to query [%s]", e, query);
        }

        LOG.debug("Return {} for query {}", rs, query);
        return rs;
    }

    protected List<StringBuilder> query2Select(String table, Query query) {
        // Build query
        StringBuilder select = new StringBuilder(128);
        select.append("SELECT ");

        // Set aggregate
        Aggregate aggr = query.aggregate();
        if (aggr != null) {
            select.append(aggr.toString());
        } else {
            select.append("*");
        }

        // Set table
        select.append(" FROM ").append(table);

        // Is query by id?
        List<StringBuilder> ids = this.queryId2Select(query, select);

        List<StringBuilder> selections;

        if (query.conditions().isEmpty()) {
            // Query only by id
            LOG.debug("Query only by id(s): {}", ids);
            selections = ids;
        } else {
            ConditionQuery condQuery = (ConditionQuery) query;
            if (condQuery.containsScanCondition()) {
                assert ids.size() == 1;
                return ImmutableList.of(queryByRange(condQuery, ids.get(0)));
            }

            selections = new ArrayList<>(ids.size());
            for (StringBuilder selection : ids) {
                // Query by condition
                selections.addAll(this.queryCondition2Select(query, selection));
            }
            LOG.debug("Query by conditions: {}", selections);
        }
        // Set page, order-by and limit
        for (StringBuilder selection : selections) {
            boolean hasOrder = !query.orders().isEmpty();
            if (hasOrder) {
                this.wrapOrderBy(selection, query);
            }
            if (query.paging()) {
                this.wrapPage(selection, query, false);
                wrapLimit(selection, query);
            } else {
                if (aggr == null && !hasOrder) {
                    select.append(this.orderByKeys());
                }
                if (!query.noLimit() || query.offset() >0L) {
                    this.wrapOffset(selection, query);
                }
            }
        }

        return selections;
    }

    protected void wrapOffset(StringBuilder select, Query query) {
        assert query.limit() >= 0;
        assert query.offset() >= 0;
        // Set limit and offset
        select.append(" LIMIT ");
        select.append(query.limit());
        select.append(" OFFSET ");
        select.append(query.offset());
        select.append(";");

        query.goOffset(query.offset());
    }

    protected void wrapOrderBy(StringBuilder select, Query query) {
        int size = query.orders().size();
        assert size > 0;

        int i = 0;
        // Set order-by
        select.append(" ORDER BY ");
        for (Map.Entry<HugeKeys, Query.Order> order : query.orders().entrySet()) {
            String key = formatKey(order.getKey());
            Query.Order value = order.getValue();
            select.append(key).append(" ");
            if (value == Query.Order.ASC) {
                select.append("ASC");
            } else {
                assert value == Query.Order.DESC;
                select.append("DESC");
            }
            if (++i != size) {
                select.append(", ");
            }
        }
    }

    protected List<StringBuilder> queryCondition2Select(Query query, StringBuilder select) {
        // Query by condition
        Set<Condition> conditions = query.conditions();
        List<StringBuilder> clauses = new ArrayList<>(conditions.size());
        for (Condition condition : conditions) {
            clauses.add(this.condition2Sql(condition));
        }
        WhereBuilder where = this.newWhereBuilder();
        where.and(clauses);
        select.append(where.build());
        return ImmutableList.of(select);
    }

    protected StringBuilder condition2Sql(Condition condition) {
        switch (condition.type()) {
            case AND:
                Condition.And and = (Condition.And) condition;
                StringBuilder left = this.condition2Sql(and.left());
                StringBuilder right = this.condition2Sql(and.right());
                int size = left.length() + right.length() + " AND ".length();
                StringBuilder sql = new StringBuilder(size);
                sql.append(left).append(" AND ").append(right);
                return sql;
            case OR:
                throw new BackendException("Not support OR currently");
            case RELATION:
                Condition.Relation r = (Condition.Relation) condition;
                return this.relation2Sql(r);
            default:
                final String msg = "Unsupported condition: " + condition;
                throw new AssertionError(msg);
        }
    }

    protected StringBuilder relation2Sql(Condition.Relation relation) {
        String key = relation.serialKey().toString();
        Object value = relation.serialValue();

        WhereBuilder sql = this.newWhereBuilder(false);
        sql.relation(key, relation.relation(), value);
        return sql.build();
    }

    protected StringBuilder queryByRange(ConditionQuery query,
                                         StringBuilder select) {
        E.checkArgument(query.relations().size() == 1,
                "Invalid scan with multi conditions: %s", query);
        Condition.Relation scan = query.relations().iterator().next();
        Shard shard = (Shard) scan.value();

        String page = query.page();
        if (ClickhouseShardSpliter.START.equals(shard.start()) &&
                ClickhouseShardSpliter.END.equals(shard.end()) &&
                (page == null || page.isEmpty())) {
            this.wrapLimit(select, query);
            return select;
        }

        HugeKeys partitionKey = this.idColumnName().get(0);

        if (page != null && !page.isEmpty()) {
            // >= page
            this.wrapPage(select, query, true);
            // < end
            WhereBuilder where = this.newWhereBuilder(false);
            if (!ClickhouseShardSpliter.END.equals(shard.end())) {
                where.and();
                where.lt(formatKey(partitionKey), shard.end());
            }
            select.append(where.build());
        } else {
            // >= start
            WhereBuilder where = this.newWhereBuilder();
            boolean hasStart = false;
            if (!ClickhouseShardSpliter.START.equals(shard.start())) {
                where.gte(formatKey(partitionKey), shard.start());
                hasStart = true;
            }
            // < end
            if (!ClickhouseShardSpliter.END.equals(shard.end())) {
                if (hasStart) {
                    where.and();
                }
                where.lt(formatKey(partitionKey), shard.end());
            }
            select.append(where.build());
        }
        this.wrapLimit(select, query);

        return select;
    }

    protected void wrapPage(StringBuilder select, Query query, boolean scan) {
        String page = query.page();
        // It's the first time if page is empty
        if (!page.isEmpty()) {
            byte[] position = PageState.fromString(page).position();
            Map<HugeKeys, Object> columns = PagePosition
                    .fromBytes(position).columns();

            List<HugeKeys> idColumnNames = this.idColumnName();
            List<Object> values = new ArrayList<>(idColumnNames.size());
            for (HugeKeys key : idColumnNames) {
                values.add(columns.get(key));
            }

            // Need add `where` to `select` when query is IdQuery
            boolean exceptWhere = scan || query.conditions().isEmpty();
            WhereBuilder where = this.newWhereBuilder(exceptWhere);
            if (!exceptWhere) {
                where.and();
            }
            where.gte(formatKeys(idColumnNames), values);
            select.append(where.build());
        }
    }

    private void wrapLimit(StringBuilder select, Query query) {
        select.append(this.orderByKeys());
        if (!query.noLimit()) {
            // Fetch `limit + 1` rows for judging whether reached the last page
            select.append(" limit ");
            select.append(query.limit() + 1);
        }
    }

    // TODO order shoud be specified, since Clickhouse do not ensure
    // returning data with same order each time
    protected String orderByKeys() {
        return Strings.EMPTY;
    }

    protected List<StringBuilder> queryId2Select(Query query, StringBuilder select) {
        // Query by id(s)
        if (query.ids().isEmpty()) {
            return ImmutableList.of(select);
        }

        List<HugeKeys> nameParts = this.idColumnName();

        List<List<Object>> ids = new ArrayList<>(query.ids().size());
        for (Id id : query.ids()) {
            List<Object> idParts = this.idColumnValue(id);
            if (nameParts.size() != idParts.size()) {
                throw new NotFoundException(
                        "Unsupported ID format '%s' (should contain %s)",
                        id, nameParts);
            }
            ids.add(idParts);
        }

        // Query only by partition-key (primary-key)
        if (nameParts.size() == 1) {
            List<Object> values = new ArrayList<>(ids.size());
            for (List<Object> objects : ids) {
                assert objects.size() == 1;
                values.add(objects.get(0));
            }

            WhereBuilder where = this.newWhereBuilder();
            where.in(formatKey(nameParts.get(0)), values);
            select.append(where.build());
            return ImmutableList.of(select);
        }

        List<StringBuilder> selections = new ArrayList<>(ids.size());
        for (List<Object> objects : ids) {
            assert nameParts.size() == objects.size();
            StringBuilder idSelection = new StringBuilder(select);
            /*
             * NOTE: concat with AND relation, like:
             * "pk = id and ck1 = v1 and ck2 = v2"
             */
            WhereBuilder where = this.newWhereBuilder();
            where.and(formatKeys(nameParts), objects);

            idSelection.append(where.build());
            selections.add(idSelection);
        }
        return selections;
    }


    protected String buildDeleteTemplate(List<HugeKeys> idNames) {
        if (this.deleteTemplate != null) {
            return this.deleteTemplate;
        }

        StringBuilder delete = new StringBuilder();
        delete.append("INSERT INTO ").append(this.table());

        ColumnListBuilder columnList = this.newColumnListBuilder();
        columnList.columnList(columnName(), generateExceptions());
        delete.append(columnList.build());

        ProjectionBuilder projection = this.newProjectionBuilder();
        projection.projection(generateDeletedReplacement(), generateExceptions());
        delete.append(projection.build());

        WhereBuilder where = this.newWhereBuilder();
        where.and(formatKeys(idNames), "=");
        delete.append(where.build());

        this.deleteTemplate = delete.toString();
        return this.deleteTemplate;
    }

    protected BackendEntry mergeEntries(BackendEntry e1, BackendEntry e2) {
        // Return the next entry (not merged)
        return e2;
    }

    protected Map<HugeKeys, Object> generateDeletedReplacement() {
        Map<HugeKeys, Object> replacements = Maps.newHashMap();
        replacements.put(HugeKeys.DELETED, 1);
        return replacements;
    }

    protected Set<HugeKeys> generateExceptions() {
        Set<HugeKeys> exceptions = Sets.newHashSet();
        exceptions.add(HugeKeys.UPDATE_NANO);
        return exceptions;
    }

    protected ColumnListBuilder newColumnListBuilder() {
        return this.newColumnListBuilder(true);
    }

    protected ColumnListBuilder newColumnListBuilder(boolean withParentheses) {
        return new ColumnListBuilder(withParentheses);
    }

    protected ProjectionBuilder newProjectionBuilder() {
        return this.newProjectionBuilder(true);
    }

    protected ProjectionBuilder newProjectionBuilder(boolean startWithSelect) {
        return new ProjectionBuilder(startWithSelect);
    }

    protected WhereBuilder newWhereBuilder() {
        return this.newWhereBuilder(true);
    }

    protected WhereBuilder newWhereBuilder(boolean startWithWhere) {
        return new WhereBuilder(startWithWhere);
    }

    protected List<Object> idColumnValue(Id id) {
        return ImmutableList.of(id.asObject());
    }

    protected List<Long> idColumnValue(ClickhouseBackendEntry.Row entry) {
        return ImmutableList.of(entry.id().asLong());
    }

    protected static String formatKey(HugeKeys key) {
        return key.name();
    }

    protected List<HugeKeys> idColumnName() {
        return this.tableDefine().keys();
    }

    protected List<HugeKeys> columnName() {
        return this.tableDefine().columns().entrySet().stream()
                .map(Map.Entry::getKey).collect(Collectors.toList());
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

    private class ProjectionBuilder {

        private StringBuilder builder;

        public ProjectionBuilder() {
            this(false);
        }

        public ProjectionBuilder(boolean startWithSelect) {
            this.builder = new StringBuilder();
            if (startWithSelect) {
                this.builder.append(" SELECT * ");
            } else {
                this.builder.append(" * ");
            }
        }

        public ProjectionBuilder projection(Map<HugeKeys, Object> replacements, Set<HugeKeys> exceptions) {
            replacements.entrySet().forEach(entry -> {
                String column = entry.getKey().string();
                Object value = entry.getKey();
                if (value instanceof String) {
                    value = String.format("'%s'", value);
                }
                builder.append(String.format("REPLACE %s AS %s ", value, column));
            });
            exceptions.forEach(column ->
                    builder.append(String.format("EXCEPT %s ", column.string())));
            return this;
        }

        public StringBuilder build() {
            return this.builder;
        }
    }

    private class ColumnListBuilder {

        private StringBuilder builder;
        private boolean withParentheses;

        public ColumnListBuilder() {
            this(false);
        }

        public ColumnListBuilder(boolean withParentheses) {
            this.withParentheses = withParentheses;
            this.builder = new StringBuilder();
            if (withParentheses) {
                this.builder.append(" (");
            }
        }

        public ColumnListBuilder columnList(List<HugeKeys> columns, Set<HugeKeys> exceptions) {
            columns = columns.stream().filter(column -> !exceptions.contains(column))
                    .collect(Collectors.toList());
            int size = columns.size();
            for (int i = 0; i < size; ++i) {
                this.builder.append(columns.get(i).string());
                if (i != size - 1) {
                    this.builder.append(",");
                }
            }
            if (withParentheses) {
                this.builder.append(")");
            }
            return this;
        }

        public StringBuilder build() {
            return this.builder;
        }
    }
}
