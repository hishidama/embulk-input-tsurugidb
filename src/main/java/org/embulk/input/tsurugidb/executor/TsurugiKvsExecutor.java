package org.embulk.input.tsurugidb.executor;

import static java.util.Locale.ENGLISH;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.embulk.config.ConfigException;
import org.embulk.input.tsurugidb.TsurugiColumn;
import org.embulk.input.tsurugidb.TsurugiInputConnection;
import org.embulk.input.tsurugidb.TsurugiInputPlugin.PluginTask;
import org.embulk.input.tsurugidb.TsurugiLiteral;
import org.embulk.input.tsurugidb.TsurugiQuerySchema;
import org.embulk.input.tsurugidb.getter.ColumnGetter;
import org.embulk.input.tsurugidb.select.BatchSelect;
import org.embulk.input.tsurugidb.select.TsurugiBatchSelectScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.tsurugidb.kvs.proto.KvsTransaction.Priority;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.CommitType;
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.RecordCursor;
import com.tsurugidb.tsubakuro.kvs.ScanBound;
import com.tsurugidb.tsubakuro.kvs.TransactionHandle;
import com.tsurugidb.tsubakuro.kvs.TransactionOption;

public class TsurugiKvsExecutor implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(TsurugiKvsExecutor.class);

    public static TsurugiKvsExecutor create(TsurugiInputConnection con, Session session) {
        var task = con.getTask();
        var txOption = getTxOption(task);
        logger.debug("txOption={}", txOption);
        var commitType = task.getCommitType().toKvsCommitType();
        logger.debug("commitType={}", commitType);

        return new TsurugiKvsExecutor(con, session, txOption, commitType);
    }

    private static TransactionOption getTxOption(PluginTask task) {
        TransactionOption.Builder builder;

        String txType = task.getTxType();
        switch (txType.toUpperCase()) {
        case "OCC":
            builder = TransactionOption.forShortTransaction();
            break;
        case "LTX":
            var ltxBuilder = TransactionOption.forLongTransaction();
            builder = ltxBuilder;
            String table = task.getTable().get();
            ltxBuilder.addWritePreserve(table);
            ltxBuilder.addInclusiveReadArea(table);
            task.getTxWritePreserve().forEach(tableName -> {
                ltxBuilder.addWritePreserve(tableName);
            });
            task.getTxInclusiveReadArea().forEach(tableName -> {
                ltxBuilder.addInclusiveReadArea(tableName);
            });
            task.getTxExclusiveReadArea().forEach(tableName -> {
                ltxBuilder.addExclusiveReadArea(tableName);
            });
            fillPriority(builder, task);
            break;
        case "RTX":
            var rtxBuilder = TransactionOption.forReadOnlyTransaction();
            builder = rtxBuilder;
            fillPriority(builder, task);
            break;
        default:
            throw new ConfigException("unsupported tx_type(" + txType + "). choose from OCC,LTX,RTX");
        }

//TODO Tsurugi KVS        builder.withLabel(task.getTxLabel());
        return builder.build();
    }

    private static void fillPriority(TransactionOption.Builder builder, PluginTask task) {
        task.getTxPriority().ifPresent(s -> {
            Priority priority;
            try {
                priority = Priority.valueOf(s.toUpperCase());
            } catch (Exception e) {
                var ce = new ConfigException(MessageFormat.format("Unknown tx_priority ''{0}''. Supported tx_priority are {1}", //
                        s, Arrays.stream(Priority.values()).map(Priority::toString).map(String::toLowerCase).collect(Collectors.joining(", "))));
                ce.addSuppressed(e);
                throw ce;
            }
            builder.withPriority(priority);
        });
    }

    private final TsurugiInputConnection con;
    private final PluginTask task;
    private final KvsClient kvsClient;
    private final TransactionOption txOption;
    private final CommitType commitType;
    private TransactionHandle transactionHandle;

    public TsurugiKvsExecutor(TsurugiInputConnection con, Session session, TransactionOption txOption, CommitType commitType) {
        this.con = con;
        this.task = con.getTask();
        this.kvsClient = KvsClient.attach(session);
        this.txOption = txOption;
        this.commitType = commitType;
    }

    public void setupTask(PluginTask task) throws ServerException {
        if (task.getTable().isEmpty()) {
            throw new ConfigException("'table' parameter is required if 'method' parameter is 'scan'.");
        }
        String tableName = task.getTable().get();

        TsurugiQuerySchema querySchema;
        PreparedQuery preparedQuery;
        if (task.getIncremental()) {
            // build incremental query

            List<String> incrementalColumns = task.getIncrementalColumns();
            if (incrementalColumns.isEmpty()) {
                // get primary keys from the target table to use them as incremental_columns
                List<String> primaryKeys = con.getPrimaryKeys(task.getTable().get());
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

            querySchema = getSchemaOfTableMetadata(tableName);
            preparedQuery = rebuildIncrementalQuery(tableName, querySchema, incrementalColumns, lastRecord);
        } else {
            querySchema = getSchemaOfTableMetadata(tableName);
            preparedQuery = new PreparedQuery(tableName, null, List.of());
        }

        task.setQuerySchema(querySchema);
        // query schema should not change after incremental query
        task.setBuiltQuery(preparedQuery);
    }

    private TsurugiQuerySchema getSchemaOfTableMetadata(String tableName) throws ServerException {
        var metadata = con.findTableMetadata(tableName).get();
        final var metadataColumns = metadata.getColumns();
        final var columns = new ArrayList<TsurugiColumn>(metadataColumns.size());
        for (var column : metadataColumns) {
            String name = column.getName();
            String typeName = TsurugiColumn.getTypeName(column);
            AtomType sqlType = column.getAtomType();
            int precision = TsurugiColumn.getPrecisionAsInt(column);
            int scale = TsurugiColumn.getScaleAsInt(column);
            columns.add(new TsurugiColumn(name, typeName, sqlType, precision, scale));
        }

        return new TsurugiQuerySchema(Collections.unmodifiableList(columns));
    }

    private PreparedQuery rebuildIncrementalQuery(String tableName, TsurugiQuerySchema querySchema, List<String> incrementalColumns, List<JsonNode> incrementalValues) {
        final var parameters = new ArrayList<TsurugiLiteral>();

        var rightLiterals = new ArrayList<TsurugiLiteral>();
        for (int n = 0; n < incrementalColumns.size(); n++) {
            String columnName = incrementalColumns.get(n);
            AtomType type = querySchema.getColumnType(columnName);
            JsonNode value = incrementalValues.get(n);
            rightLiterals.add(new TsurugiLiteral(columnName, type, value));
        }

        return new PreparedQuery(tableName, null, parameters);
    }

    public BatchSelect newBatchSelect(PreparedQuery preparedQuery, Map<String, ColumnGetter> getters, int fetchRows, int queryTimeout) throws ServerException {
        String tableName = preparedQuery.getTableName();

        List<TsurugiLiteral> parameters = preparedQuery.getParameters();
        var lowerKey = new RecordBuffer();
        for (var literal : parameters) {
            String name = literal.getColumnName();
            ColumnGetter getter = getters.get(name);
            getter.decodeFromJsonTo(lowerKey, name, literal.getValue());
        }

        return new TsurugiBatchSelectScan(this, tableName, lowerKey);
    }

    public RecordCursor executeScan(String tableName, RecordBuffer lowerKey) throws IOException, ServerException, InterruptedException, TimeoutException {
        var tx = getTransaction();
        var lowerBound = ScanBound.EXCLUSIVE;
        if (lowerKey == null || lowerKey.size() == 0) {
            lowerKey = null;
            lowerBound = null;
        }
        int timeout = task.getSelectTimeout();
        return kvsClient.scan(tx, tableName, lowerKey, lowerBound, null, null).await(timeout, TimeUnit.SECONDS);
    }

    private synchronized TransactionHandle getTransaction() throws ServerException {
        if (transactionHandle == null) {
            int timeout = task.getBeginTimeout();
            try {
                transactionHandle = kvsClient.beginTransaction(txOption).await(timeout, TimeUnit.SECONDS);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            } catch (InterruptedException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
        return transactionHandle;
    }

    public synchronized void commit() throws ServerException {
        if (transactionHandle == null) {
            return;
        }

        try {
            int timeout = task.getCommitTimeout();
            kvsClient.commit(transactionHandle, commitType).await(timeout, TimeUnit.SECONDS);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }

        try {
            transactionHandle.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        transactionHandle = null;
    }

    @Override
    public void close() throws ServerException {
        try (kvsClient; var t = transactionHandle) {
            // close only
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
