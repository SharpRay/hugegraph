package com.mininglamp.hugegraph.backend.store.clickhouse;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Aggregate;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.TableBackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendTable;
import com.baidu.hugegraph.backend.store.Shard;
import com.baidu.hugegraph.backend.store.TableDefine;
import com.baidu.hugegraph.backend.store.mysql.WhereBuilder;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.mininglamp.hugegraph.backend.store.clickhouse.ClickhouseSessions.Session;
import com.baidu.hugegraph.util.Log;
import org.apache.commons.compress.utils.Lists;
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
        sql.append(")")
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

    @Override
    public Iterator<BackendEntry> query(Session session, Query query) {
        return this.query(session, query, this::results2Entries);
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
