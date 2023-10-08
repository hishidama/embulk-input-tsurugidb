package org.embulk.input.tsurugidb.executor;

import static java.util.Locale.ENGLISH;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.embulk.config.ConfigException;
import org.embulk.input.tsurugidb.TsurugiColumn;
import org.embulk.input.tsurugidb.TsurugiInputPlugin.PluginTask;
import org.embulk.input.tsurugidb.TsurugiLiteral;
import org.embulk.input.tsurugidb.TsurugiQuerySchema;
import org.embulk.input.tsurugidb.getter.ColumnGetter;
import org.embulk.input.tsurugidb.select.BatchSelect;
import org.embulk.input.tsurugidb.select.TsurugiSingleSelect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.sql.proto.SqlRequest.CommitStatus;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.sql.proto.SqlRequest.Placeholder;
import com.tsurugidb.sql.proto.SqlRequest.ReadArea;
import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.sql.proto.SqlRequest.TransactionPriority;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;
import com.tsurugidb.sql.proto.SqlRequest.WritePreserve;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.StatementMetadata;
import com.tsurugidb.tsubakuro.sql.TableMetadata;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.sql.exception.TargetNotFoundException;

// https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/JdbcInputConnection.java
public class TsurugiSqlExecutor implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(TsurugiSqlExecutor.class);

    public static TsurugiSqlExecutor create(PluginTask task, Session session) {
        var txOption = getTxOption(task);
        logger.debug("txOption={}", txOption);
        var commitStatus = task.getCommitType().toCommitStatus();
        logger.debug("commitStatus={}", commitStatus);

        return new TsurugiSqlExecutor(task, session, txOption, commitStatus);
    }

    private static TransactionOption getTxOption(PluginTask task) {
        var builder = TransactionOption.newBuilder();

        String txType = task.getTxType();
        switch (txType.toUpperCase()) {
        case "OCC":
            builder.setType(TransactionType.SHORT);
            break;
        case "LTX":
            builder.setType(TransactionType.LONG);
            task.getTxWritePreserve().forEach(tableName -> {
                builder.addWritePreserves(WritePreserve.newBuilder().setTableName(tableName));
            });
            task.getTable().ifPresent(tableName -> {
                builder.addInclusiveReadAreas(ReadArea.newBuilder().setTableName(tableName));
            });
            task.getTxInclusiveReadArea().forEach(tableName -> {
                builder.addInclusiveReadAreas(ReadArea.newBuilder().setTableName(tableName));
            });
            task.getTxExclusiveReadArea().forEach(tableName -> {
                builder.addExclusiveReadAreas(ReadArea.newBuilder().setTableName(tableName));
            });
            fillPriority(builder, task);
            break;
        case "RTX":
            builder.setType(TransactionType.READ_ONLY);
            fillPriority(builder, task);
            break;
        default:
            throw new ConfigException("unsupported tx_type(" + txType + "). choose from OCC,LTX,RTX");
        }

        builder.setLabel(task.getTxLabel());
        return builder.build();
    }

    private static void fillPriority(TransactionOption.Builder builder, PluginTask task) {
        task.getTxPriority().ifPresent(s -> {
            TransactionPriority priority;
            try {
                priority = TransactionPriority.valueOf(s.toUpperCase());
            } catch (Exception e) {
                var ce = new ConfigException(MessageFormat.format("Unknown tx_priority ''{0}''. Supported tx_priority are {1}", //
                        s, Arrays.stream(TransactionPriority.values()).map(TransactionPriority::toString).map(String::toLowerCase).collect(Collectors.joining(", "))));
                ce.addSuppressed(e);
                throw ce;
            }
            builder.setPriority(priority);
        });
    }

    private final PluginTask task;
    private final SqlClient sqlClient;
    private final TransactionOption txOption;
    private final CommitStatus commitStatus;
    private Transaction transaction = null;
    protected String identifierQuoteString = "\"";

    public TsurugiSqlExecutor(PluginTask task, Session session, TransactionOption txOption, CommitStatus commitStatus) {
        this.task = task;
        this.sqlClient = SqlClient.attach(session);
        this.txOption = txOption;
        this.commitStatus = commitStatus;
    }

    public void setupTask(PluginTask task) throws ServerException {
        // build SELECT query and gets schema of its result
        String rawQuery = getRawQuery(task);

        TsurugiQuerySchema querySchema;
        PreparedQuery preparedQuery;
        if (task.getIncremental()) {
            // build incremental query

            List<String> incrementalColumns = task.getIncrementalColumns();
            if (incrementalColumns.isEmpty()) {
                // incremental_columns is not set
                if (!task.getTable().isPresent()) {
                    throw new ConfigException("incremental_columns option must be set if incremental is true and custom query option is set");
                }
                // get primary keys from the target table to use them as incremental_columns
                List<String> primaryKeys = getPrimaryKeys(task.getTable().get());
                if (primaryKeys.isEmpty()) {
                    throw new ConfigException(String.format(ENGLISH, "Primary key is not available at the table '%s'. incremental_columns option must be set", task.getTable().get()));
                }
                logger.info("Using primary keys as incremental_columns: {}", primaryKeys);
                task.setIncrementalColumns(primaryKeys);
                incrementalColumns = primaryKeys;
            }

            List<JsonNode> lastRecord;
            if (task.getLastRecord().isPresent()) {
                lastRecord = task.getLastRecord().get();
                if (lastRecord.size() != incrementalColumns.size()) {
                    throw new ConfigException("Number of values set at last_record must be same with number of columns set at incremental_columns");
                }
            } else {
                lastRecord = null;
            }

            if (task.getQuery().isPresent()) {
                querySchema = getSchemaOfQuery(rawQuery, incrementalColumns);
                preparedQuery = wrapIncrementalQuery(rawQuery, querySchema, incrementalColumns, lastRecord, task.getUseRawQueryWithIncremental());
            } else {
                querySchema = getSchemaOfQuery(rawQuery, incrementalColumns);
                preparedQuery = rebuildIncrementalQuery(task.getTable().get(), task.getSelect(), task.getWhere(), querySchema, incrementalColumns, lastRecord);
            }
        } else {
            querySchema = getSchemaOfQuery(rawQuery, List.of());
            preparedQuery = new PreparedQuery(null, rawQuery, List.of());
        }

        task.setQuerySchema(querySchema);
        // query schema should not change after incremental query
        task.setBuiltQuery(preparedQuery);
    }

    private String getRawQuery(PluginTask task) {
        if (task.getQuery().isPresent()) {
            if (task.getTable().isPresent() || task.getSelect().isPresent() || task.getWhere().isPresent() || task.getOrderBy().isPresent()) {
                throw new ConfigException("'table', 'select', 'where' and 'order_by' parameters are unnecessary if 'query' parameter is set.");
            } else if (task.getUseRawQueryWithIncremental()) {
                String rawQuery = task.getQuery().get();
                for (String columnName : task.getIncrementalColumns()) {
                    if (!rawQuery.contains(":" + columnName)) {
                        throw new ConfigException(String.format("Column \":%s\" doesn't exist in query string", columnName));
                    }
                }
                if (!task.getLastRecord().isPresent()) {
                    throw new ConfigException("'last_record' is required when 'use_raw_query_with_incremental' is set to true");
                }
                if (task.getLastRecord().get().size() != task.getIncrementalColumns().size()) {
                    throw new ConfigException("size of 'last_record' is different from of 'incremental_columns'");
                }
            } else if (!task.getUseRawQueryWithIncremental() && (!task.getIncrementalColumns().isEmpty() || task.getLastRecord().isPresent())) {
                throw new ConfigException("'incremental_columns' and 'last_record' parameters are not supported if 'query' parameter is set and 'use_raw_query_with_incremental' is set to false.");
            }
            return task.getQuery().get();
        } else if (task.getTable().isPresent()) {
            return buildSelectQuery(task.getTable().get(), task.getSelect(), task.getWhere(), task.getOrderBy());
        } else {
            throw new ConfigException("'table' or 'query' parameter is required");
        }
    }

    private TsurugiQuerySchema getSchemaOfQuery(String query, List<String> incrementalColumns) throws ServerException {
        var placeholders = incrementalColumns.stream() //
                .map(name -> Placeholders.of(name, AtomType.CHARACTER)) //
                .collect(Collectors.toList());
        int timeout = task.getSelectTimeout();
        try (var ps = sqlClient.prepare(query, placeholders).await(timeout, TimeUnit.SECONDS)) {
            var parameters = incrementalColumns.stream() //
                    .map(name -> Parameters.ofNull(name)) //
                    .collect(Collectors.toList());
            var metadata = sqlClient.explain(ps, parameters).await(timeout, TimeUnit.SECONDS);
            return getSchemaOfResultMetadata(metadata);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> getPrimaryKeys(String tableName) throws ServerException {
        logger.warn("TsurugiSQLConnection.getPrimaryKeys() is not yet supported");
        final var primaryKeys = new ArrayList<String>();
        var metadata = findTableMetadata(tableName).get();
        for (@SuppressWarnings("unused")
        var column : metadata.getColumns()) {
            // TODO Tsurugi primary key
        }
        return Collections.unmodifiableList(primaryKeys);
    }

    protected TsurugiQuerySchema getSchemaOfResultMetadata(StatementMetadata metadata) {
        final var metadataColumns = metadata.getColumns();
        final var columns = new ArrayList<TsurugiColumn>(metadataColumns.size());
        int i = 0;
        for (var column : metadataColumns) {
            String name = column.getName();
            if (name.isEmpty()) {
                name = "@#" + i;
            }
            // TODO Tsurugi get metadata
//          String typeName = metadata.getColumnTypeName(index);
            String typeName = "";
//          int sqlType = metadata.getColumnType(index);
            AtomType sqlType = column.getAtomType();
//          int scale = metadata.getScale(index);
            int scale = 15;
//          int precision = metadata.getPrecision(index);
            int precision = 0;
            columns.add(new TsurugiColumn(name, typeName, sqlType, precision, scale));

            i++;
        }

        return new TsurugiQuerySchema(Collections.unmodifiableList(columns));
    }

    public BatchSelect newBatchSelect(PreparedQuery preparedQuery, Map<String, ColumnGetter> getters, int fetchRows, int queryTimeout) throws ServerException {
        String query = preparedQuery.getQuery();
        List<Placeholder> placeholders = List.of(); // TODO Tsurugi implements List<Placeholder>
        List<TsurugiLiteral> params = preparedQuery.getParameters();

        PreparedStatement stmt;
        try {
            int timeout = task.getSelectTimeout();
            stmt = sqlClient.prepare(query, placeholders).await(timeout, TimeUnit.SECONDS);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
//TODO  Tsurugi     stmt.setFetchSize(fetchRows);
//TODO  Tsurugi     stmt.setQueryTimeout(queryTimeout);
        logger.info("SQL: " + query);
        return new TsurugiSingleSelect(this, stmt, prepareParameters(getters, params));
    }

    protected List<Parameter> prepareParameters(Map<String, ColumnGetter> getters, List<TsurugiLiteral> parameters) {
        var parameterList = new ArrayList<Parameter>();

        for (var literal : parameters) {
            String name = literal.getColumnName();
            ColumnGetter getter = getters.get(name);
            getter.decodeFromJsonTo(parameterList, name, literal.getValue());
        }

        return parameterList;
    }

    @Override
    public void close() throws ServerException {
        try (sqlClient; var t = transaction) {
            // close only
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void executeUpdate(String sql) throws ServerException {
        var tx = getTransaction();
        try {
            int timeout = task.getUpdateTimeout();
            tx.executeStatement(sql).await(timeout, TimeUnit.SECONDS);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    // TODO share code with embulk-output-jdbc
    protected String quoteIdentifierString(String str) {
        return identifierQuoteString + str + identifierQuoteString;
    }

    protected String buildTableName(String tableName) {
        return quoteIdentifierString(tableName);
    }

    protected String buildSelectQuery(String tableName, Optional<String> selectExpression, Optional<String> whereCondition, Optional<String> orderByExpression) {
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT ");
        sb.append(selectExpression.orElse("*"));
        sb.append(" FROM ").append(buildTableName(tableName));

        if (whereCondition.isPresent()) {
            sb.append(" WHERE ").append(whereCondition.get());
        }

        if (orderByExpression.isPresent()) {
            sb.append(" ORDER BY ").append(orderByExpression.get());
        }

        return sb.toString();
    }

    private PreparedQuery rebuildIncrementalQuery(String tableName, Optional<String> selectExpression, Optional<String> whereCondition, TsurugiQuerySchema querySchema, List<String> incrementalColumns,
            List<JsonNode> incrementalValues) {
        List<TsurugiLiteral> parameters = List.of();

        Optional<String> newWhereCondition;
        if (incrementalValues != null) {
            StringBuilder sb = new StringBuilder();

            if (whereCondition.isPresent()) {
                sb.append("(");
                sb.append(whereCondition.get());
                sb.append(") AND ");
            }

            sb.append("(");
            parameters = buildIncrementalConditionTo(sb, querySchema, incrementalColumns, incrementalValues);
            sb.append(")");

            newWhereCondition = Optional.of(sb.toString());
        } else {
            newWhereCondition = whereCondition;
        }

        Optional<String> newOrderByExpression;
        {
            StringBuilder sb = new StringBuilder();
            buildIncrementalOrderTo(sb, incrementalColumns);
            newOrderByExpression = Optional.of(sb.toString());
        }

        String newQuery = buildSelectQuery(tableName, selectExpression, newWhereCondition, newOrderByExpression);

        return new PreparedQuery(null, newQuery, parameters);
    }

    private PreparedQuery wrapIncrementalQuery(String rawQuery, TsurugiQuerySchema querySchema, List<String> incrementalColumns, List<JsonNode> incrementalValues, boolean useRawQuery) {
        StringBuilder sb = new StringBuilder();
        List<TsurugiLiteral> parameters = List.of();

        if (useRawQuery) {
            parameters = replacePlaceholder(sb, rawQuery, querySchema, incrementalColumns, incrementalValues);
        } else {
            sb.append("SELECT * FROM (");
            sb.append(truncateStatementDelimiter(rawQuery));
            sb.append(") embulk_incremental_");

            if (incrementalValues != null) {
                sb.append(" WHERE ");
                parameters = buildIncrementalConditionTo(sb, querySchema, incrementalColumns, incrementalValues);
            }

            sb.append(" ORDER BY ");
            buildIncrementalOrderTo(sb, incrementalColumns);
        }

        return new PreparedQuery(null, sb.toString(), parameters);
    }

    private List<TsurugiLiteral> buildIncrementalConditionTo(StringBuilder sb, TsurugiQuerySchema querySchema, List<String> incrementalColumns, List<JsonNode> incrementalValues) {
        final var parameters = new ArrayList<TsurugiLiteral>();

        var leftColumnNames = new ArrayList<String>();
        var rightLiterals = new ArrayList<TsurugiLiteral>();
        for (int n = 0; n < incrementalColumns.size(); n++) {
            String columnName = incrementalColumns.get(n);
            AtomType type = querySchema.getColumnType(columnName);
            JsonNode value = incrementalValues.get(n);
            leftColumnNames.add(columnName);
            rightLiterals.add(new TsurugiLiteral(columnName, type, value));
        }

        for (int n = 0; n < leftColumnNames.size(); n++) {
            if (n > 0) {
                sb.append(" OR ");
            }
            sb.append("(");

            for (int i = 0; i < n; i++) {
                sb.append(quoteIdentifierString(leftColumnNames.get(i)));
                sb.append(" = :");
                sb.append(leftColumnNames.get(i));
                parameters.add(rightLiterals.get(i));
                sb.append(" AND ");
            }
            sb.append(quoteIdentifierString(leftColumnNames.get(n)));
            sb.append(" > :");
            sb.append(leftColumnNames.get(n));
            parameters.add(rightLiterals.get(n));

            sb.append(")");
        }

        return Collections.unmodifiableList(parameters);
    }

    private List<TsurugiLiteral> replacePlaceholder(StringBuilder sb, String rawQuery, TsurugiQuerySchema querySchema, List<String> incrementalColumns, List<JsonNode> incrementalValues) {
        final var parameters = new ArrayList<TsurugiLiteral>(incrementalColumns.size());
        for (int i = 0; i < incrementalColumns.size(); i++) {
            String columnName = incrementalColumns.get(i);
            AtomType type = querySchema.getColumnType(columnName);
            JsonNode value = incrementalValues.get(i);
            parameters.add(new TsurugiLiteral(columnName, type, value));
        }

        return Collections.unmodifiableList(parameters);
    }

    private void buildIncrementalOrderTo(StringBuilder sb, List<String> incrementalColumns) {
        boolean first = true;
        for (String incrementalColumn : incrementalColumns) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(quoteIdentifierString(incrementalColumn));
        }
    }

    protected String truncateStatementDelimiter(String rawQuery) {
        return rawQuery.replaceAll(";\\s*$", "");
    }

    public boolean tableExists(String tableName) throws ServerException {
        return findTableMetadata(tableName).isPresent();
    }

    public Optional<TableMetadata> findTableMetadata(String tableName) throws ServerException {
        int timeout = task.getSelectTimeout();
        try {
            var metadata = sqlClient.getTableMetadata(tableName).await(timeout, TimeUnit.SECONDS);
            return Optional.of(metadata);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (TargetNotFoundException e) {
            return Optional.empty();
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public ResultSet executeQuery(PreparedStatement fetchStatement, List<Parameter> parameters) throws IOException, ServerException, InterruptedException, TimeoutException {
        var tx = getTransaction();
        int timeout = task.getSelectTimeout();
        return tx.executeQuery(fetchStatement, parameters).await(timeout, TimeUnit.SECONDS);
    }

    protected synchronized Transaction getTransaction() throws ServerException {
        if (transaction == null) {
            int timeout = task.getBeginTimeout();
            try {
                transaction = sqlClient.createTransaction(txOption).await(timeout, TimeUnit.SECONDS);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            } catch (InterruptedException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
        return transaction;
    }

    public Map<String, AtomType> getIncrementalColumnType(TsurugiQuerySchema querySchema, List<String> incrementalColumns) {
        var metadataMap = querySchema.getColumns().stream().collect(Collectors.toMap(c -> c.getName(), c -> c.getSqlType()));
        var map = new LinkedHashMap<String, AtomType>();
        for (var columnName : incrementalColumns) {
            var type = metadataMap.get(columnName);
            map.put(columnName, type);
        }
        return map;
    }

    public synchronized void commit() throws ServerException {
        if (transaction == null) {
            return;
        }

        try {
            int timeout = task.getCommitTimeout();
            transaction.commit(commitStatus).await(timeout, TimeUnit.SECONDS);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }

        try {
            transaction.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        transaction = null;
    }
}
