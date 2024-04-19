package org.embulk.input.tsurugidb;

import static java.util.Locale.ENGLISH;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.tsurugidb.common.DbColumnOption;
import org.embulk.input.tsurugidb.common.ToStringMap;
import org.embulk.input.tsurugidb.executor.PreparedQuery;
import org.embulk.input.tsurugidb.getter.ColumnGetter;
import org.embulk.input.tsurugidb.getter.ColumnGetterFactory;
import org.embulk.input.tsurugidb.select.BatchSelect;
import org.embulk.input.tsurugidb.select.SelectMethod;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.modules.ZoneIdModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.tsurugidb.tsubakuro.exception.ServerException;

// https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/AbstractJdbcInputPlugin.java
public class TsurugiInputPlugin implements InputPlugin {
    private static final Logger logger = LoggerFactory.getLogger(TsurugiInputPlugin.class);

    public static final String TYPE = "tsurugidb";

    protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().addModule(ZoneIdModule.withLegacyNames()).build();
    protected static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
    protected static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();;

    public interface PluginTask extends Task {
        @Config("endpoint")
        public String getEndpoint();

        @Config("connection_label")
        @ConfigDefault("\"embulk-input-tsurugidb\"")
        public String getConnectionLabel();

        @Config("user")
        @ConfigDefault("null")
        public Optional<String> getUser();

        @Config("password")
        @ConfigDefault("null")
        public Optional<String> getPassword();

        @Config("method")
        @ConfigDefault("\"select\"")
        public SelectMethod getSelectMethod();

        @Config("tx_type")
        @ConfigDefault("\"RTX\"") // OCC, LTX, RTX
        public String getTxType();

        @Config("tx_label")
        @ConfigDefault("\"embulk-input-tsurugidb\"")
        public String getTxLabel();

        @Config("tx_write_preserve")
        @ConfigDefault("[]")
        public List<String> getTxWritePreserve();

        @Config("tx_inclusive_read_area")
        @ConfigDefault("[]")
        public List<String> getTxInclusiveReadArea();

        @Config("tx_exclusive_read_area")
        @ConfigDefault("[]")
        public List<String> getTxExclusiveReadArea();

        @Config("tx_priority")
        @ConfigDefault("null")
        public Optional<String> getTxPriority();

        @Config("commit_type")
        @ConfigDefault("\"default\"")
        public TsurugiCommitType getCommitType();

        @Config("options")
        @ConfigDefault("{}")
        public ToStringMap getOptions();

        @Config("table")
        @ConfigDefault("null")
        public Optional<String> getTable();

        public void setTable(Optional<String> normalizedTableName);

        @Config("query")
        @ConfigDefault("null")
        public Optional<String> getQuery();

        @Config("use_raw_query_with_incremental")
        @ConfigDefault("false")
        public boolean getUseRawQueryWithIncremental();

        @Config("select")
        @ConfigDefault("null")
        public Optional<String> getSelect();

        @Config("where")
        @ConfigDefault("null")
        public Optional<String> getWhere();

        @Config("order_by")
        @ConfigDefault("null")
        public Optional<String> getOrderBy();

        @Config("incremental")
        @ConfigDefault("false")
        public boolean getIncremental();

        @Config("incremental_columns")
        @ConfigDefault("[]")
        public List<String> getIncrementalColumns();

        public void setIncrementalColumns(List<String> indexes);

        @Config("last_record")
        @ConfigDefault("null")
        public Optional<List<JsonNode>> getLastRecord();

        // TODO limit_value is necessary to make sure repeated bulk load transactions
        // don't a same record twice or miss records when the column
        // specified at order_by parameter is not unique.
        // For example, if the order_by column is "timestamp created_at"
        // column whose precision is second, the table can include multiple
        // records with the same created_at time. At the first bulk load
        // transaction, it loads a record with created_at=2015-01-02 00:00:02.
        // Then next transaction will use WHERE created_at > '2015-01-02 00:00:02'.
        // However, if another record with created_at=2014-01-01 23:59:59 is
        // inserted between the 2 transactions, the new record will be skipped.
        // To prevent this scenario, we want to specify
        // limit_value=2015-01-02 00:00:00 (exclusive). With this way, as long as
        // a transaction runs after 2015-01-02 00:00:00 + some minutes, we don't
        // skip records. Ideally, to automate the scheduling, we want to set
        // limit_value="today".
        //
        // @Config("limit_value")
        // @ConfigDefault("null")
        // public Optional<String> getLimitValue();

        //// TODO probably limit_rows is unnecessary as long as this has
        // supports parallel execution (partition_by option) and resuming.
        // @Config("limit_rows")
        // @ConfigDefault("null")
        // public Optional<Integer> getLimitRows();

        @Config("connect_timeout")
        @ConfigDefault("300")
        public int getConnectTimeout();

        @Config("begin_timeout")
        @ConfigDefault("300")
        public int getBeginTimeout();

        @Config("select_timeout")
        @ConfigDefault("300")
        public int getSelectTimeout();

        @Config("update_timeout")
        @ConfigDefault("300")
        public int getUpdateTimeout();

        @Config("commit_timeout")
        @ConfigDefault("300")
        public int getCommitTimeout();

        @Config("socket_timeout")
        @ConfigDefault("1800")
        public int getSocketTimeout();

        @Config("fetch_rows")
        @ConfigDefault("10000")
        // TODO set minimum number
        public int getFetchRows();

        // TODO parallel execution using "partition_by" config

        @Config("column_options")
        @ConfigDefault("{}")
        public Map<String, DbColumnOption> getColumnOptions();

        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        public ZoneId getDefaultTimeZone();

        @Config("default_column_options")
        @ConfigDefault("{}")
        public Map<String, DbColumnOption> getDefaultColumnOptions();

        @Config("before_setup")
        @ConfigDefault("null")
        public Optional<String> getBeforeSetup();

        @Config("before_select")
        @ConfigDefault("null")
        public Optional<String> getBeforeSelect();

        @Config("after_select")
        @ConfigDefault("null")
        public Optional<String> getAfterSelect();

        public void setBuiltQuery(PreparedQuery query);

        public PreparedQuery getBuiltQuery();

        public void setQuerySchema(TsurugiQuerySchema schema);

        public TsurugiQuerySchema getQuerySchema();
    }

    protected TsurugiInputConnection newConnection(PluginTask task) throws ServerException {
        return TsurugiInputConnection.newConnection(task);
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, Control control) {
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

        if (task.getIncremental()) {
            if (task.getOrderBy().isPresent()) {
                throw new ConfigException("order_by option must not be set if incremental is true");
            }
        } else {
            if (!task.getIncrementalColumns().isEmpty()) {
                throw new ConfigException("'incremental: true' must be set if incremental_columns is set");
            }
        }

        Schema schema;
        try (var con = newConnection(task)) {
            if (task.getBeforeSetup().isPresent()) {
                var executor = con.getSqlExecutor();
                executor.executeUpdate(task.getBeforeSetup().get());
                executor.commit();
            }

            // TODO incremental_columns is not set => get primary key
            schema = setupTask(con, task);
        } catch (ServerException e) {
            throw new ServerRuntimeException(e);
        }

        return buildNextConfigDiff(task, control.run(task.toTaskSource(), schema, 1));
    }

    protected Schema setupTask(TsurugiInputConnection con, PluginTask task) throws ServerException {
        if (task.getTable().isPresent()) {
            String actualTableName = normalizeTableNameCase(con, task.getTable().get());
            task.setTable(Optional.of(actualTableName));
        }

        var selectMethod = task.getSelectMethod();
        switch (selectMethod) {
        case SELECT:
            var sqlExecutor = con.getSqlExecutor();
            sqlExecutor.setupTask(task);
            break;
        case SCAN:
            var kvsExecutor = con.getKvsExecutor();
            kvsExecutor.setupTask(task);
            break;
        default:
            throw new AssertionError(selectMethod);
        }

        var querySchema = task.getQuerySchema();

        // validate column_options
        newColumnGetters(task, querySchema, null);

        ColumnGetterFactory factory = newColumnGetterFactory(null, task.getDefaultTimeZone());
        final var columns = new ArrayList<Column>();
        for (int i = 0; i < querySchema.getCount(); i++) {
            TsurugiColumn column = querySchema.getColumn(i);
            DbColumnOption columnOption = columnOptionOf(task.getColumnOptions(), task.getDefaultColumnOptions(), column, factory.getJdbcType(column));
            columns.add(new Column(i, column.getName(), factory.newColumnGetter(column, columnOption, column.getSqlType()).getToType()));
        }
        return new Schema(Collections.unmodifiableList(columns));
    }

    private String normalizeTableNameCase(TsurugiInputConnection con, String tableName) throws ServerException {
        if (con.tableExists(tableName)) {
            return tableName;
        } else {
            String upperTableName = tableName.toUpperCase();
            String lowerTableName = tableName.toLowerCase();
            boolean upperExists = con.tableExists(upperTableName);
            boolean lowerExists = con.tableExists(lowerTableName);
            if (upperExists && lowerExists) {
                throw new ConfigException(String.format("Cannot specify table '%s' because both '%s' and '%s' exist.", tableName, upperTableName, lowerTableName));
            } else if (upperExists) {
                return upperTableName;
            } else if (lowerExists) {
                return lowerTableName;
            } else {
                // fallback to the given table name. this may throw error later at
                // getSchemaOfQuery
                return tableName;
            }
        }
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, Control control) {
        final PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);

        return buildNextConfigDiff(task, control.run(taskSource, schema, taskCount));
    }

    @Override
    public ConfigDiff guess(ConfigSource config) {
        return CONFIG_MAPPER_FACTORY.newConfigDiff();
    }

    protected ConfigDiff buildNextConfigDiff(PluginTask task, List<TaskReport> reports) {
        final ConfigDiff next = CONFIG_MAPPER_FACTORY.newConfigDiff();
        if (reports.size() > 0 && reports.get(0).has("last_record")) {
            // |reports| are from embulk-core, then their backend is Jackson on the
            // embulk-core side.
            // To render |JsonNode| (that is on the plugin side) from |reports|, they need
            // to be rebuilt.
            final TaskReport report = CONFIG_MAPPER_FACTORY.rebuildTaskReport(reports.get(0));
            next.set("last_record", report.get(JsonNode.class, "last_record"));
        } else if (task.getLastRecord().isPresent()) {
            next.set("last_record", task.getLastRecord().get());
        }
        return next;
    }

    @Override
    public void cleanup(TaskSource taskSource, Schema schema, int taskCount, List<TaskReport> successTaskReports) {
        // do nothing
    }

    private static class LastRecordStore {
        private final JsonNode[] lastValues;
        private final List<String> columnNames;

        public LastRecordStore(List<String> columnNames) {
            this.lastValues = new JsonNode[columnNames.size()];
            this.columnNames = columnNames;
        }

        public void accept(Map<String, ColumnGetter> getters) {
            int i = 0;
            for (String name : columnNames) {
                lastValues[i] = getters.get(name).encodeToJson();
                i++;
            }
        }

        public List<JsonNode> getList() {
            final var values = new ArrayList<JsonNode>();
            for (int i = 0; i < lastValues.length; i++) {
                if (lastValues[i] == null || lastValues[i].isNull()) {
                    throw new DataException(String.format(ENGLISH, "incremental_columns can't include null values but the last row is null at column '%s'", columnNames.get(i)));
                }
                values.add(lastValues[i]);
            }
            return Collections.unmodifiableList(values);
        }
    }

    @Override
    public TaskReport run(TaskSource taskSource, Schema schema, int taskIndex, PageOutput output) {
        final PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);

        PreparedQuery builtQuery = task.getBuiltQuery();
        TsurugiQuerySchema querySchema = task.getQuerySchema();
        BufferAllocator allocator = Exec.getBufferAllocator();
        PageBuilder pageBuilder = Exec.getPageBuilder(allocator, schema, output);

        long totalRows = 0;

        LastRecordStore lastRecordStore = null;

        try (TsurugiInputConnection con = newConnection(task)) {
            var executor = con.getSqlExecutor();
            if (task.getBeforeSelect().isPresent()) {
                executor.executeUpdate(task.getBeforeSelect().get());
            }

            Map<String, ColumnGetter> getters = newColumnGetters(task, querySchema, pageBuilder);
            try (BatchSelect cursor = newSelectCursor(task, con, builtQuery, getters, task.getFetchRows(), task.getSocketTimeout())) {
                while (true) {
                    long rows = cursor.fetch(getters, pageBuilder, logger);
                    if (rows <= 0L) {
                        break;
                    }
                    totalRows += rows;
                }
            }

            if (task.getIncremental() && totalRows > 0) {
                lastRecordStore = new LastRecordStore(task.getIncrementalColumns());
                lastRecordStore.accept(getters);
            }

            pageBuilder.finish();

            // after_select runs after pageBuilder.finish because pageBuilder.finish may
            // fail.
            // TODO Output plugin's transaction might still fail. In that case, after_select
            // is
            // already done but output plugin didn't commit the data to the target storage.
            // This means inconsistency between data source and destination. To avoid this
            // issue, we need another option like `after_commit` that runs after output
            // plugin's
            // commit. after_commit can't run in the same transaction with SELECT. So,
            // after_select gets values and store them in TaskReport, and after_commit take
            // them as placeholder. Or, after_select puts values to an intermediate table,
            // and
            // after_commit moves those values to the actual table.
            if (task.getAfterSelect().isPresent()) {
                executor.executeUpdate(task.getAfterSelect().get());
            }
            executor.commit();

        } catch (ServerException e) {
            throw new ServerRuntimeException(e);
        }

        final TaskReport report = CONFIG_MAPPER_FACTORY.newTaskReport();
        if (lastRecordStore != null) {
            report.set("last_record", lastRecordStore.getList());
        }

        return report;
    }

    protected BatchSelect newSelectCursor(PluginTask task, TsurugiInputConnection con, PreparedQuery preparedQuery, Map<String, ColumnGetter> getters, int fetchRows, int queryTimeout)
            throws ServerException {
        var selectMethod = task.getSelectMethod();
        switch (selectMethod) {
        case SELECT:
            return con.getSqlExecutor().newBatchSelect(preparedQuery, getters, fetchRows, queryTimeout);
        case SCAN:
            return con.getKvsExecutor().newBatchSelect(preparedQuery, getters, fetchRows, queryTimeout);
        default:
            throw new AssertionError(selectMethod);
        }
    }

    protected ColumnGetterFactory newColumnGetterFactory(PageBuilder pageBuilder, ZoneId dateTimeZone) {
        return new ColumnGetterFactory(pageBuilder, dateTimeZone);
    }

    private Map<String, ColumnGetter> newColumnGetters(PluginTask task, TsurugiQuerySchema querySchema, PageBuilder pageBuilder) {
        ColumnGetterFactory factory = newColumnGetterFactory(pageBuilder, task.getDefaultTimeZone());
        final var getters = new LinkedHashMap<String, ColumnGetter>();
        for (TsurugiColumn column : querySchema.getColumns()) {
            DbColumnOption columnOption = columnOptionOf(task.getColumnOptions(), task.getDefaultColumnOptions(), column, factory.getJdbcType(column));
            getters.put(column.getName(), factory.newColumnGetter(column, columnOption, column.getSqlType()));
        }
        return Collections.unmodifiableMap(getters);
    }

    private static DbColumnOption columnOptionOf(Map<String, DbColumnOption> columnOptions, Map<String, DbColumnOption> defaultColumnOptions, TsurugiColumn targetColumn, String targetColumnSQLType) {
        DbColumnOption columnOption = columnOptions.get(targetColumn.getName());
        if (columnOption == null) {
            String foundName = null;
            for (Map.Entry<String, DbColumnOption> entry : columnOptions.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(targetColumn.getName())) {
                    if (columnOption != null) {
                        throw new ConfigException(String.format("Cannot specify column '%s' because both '%s' and '%s' exist in column_options.", targetColumn.getName(), foundName, entry.getKey()));
                    }
                    foundName = entry.getKey();
                    columnOption = entry.getValue();
                }
            }
        }

        if (columnOption != null) {
            return columnOption;
        }
        final DbColumnOption defaultColumnOption = defaultColumnOptions.get(targetColumnSQLType);
        if (defaultColumnOption != null) {
            return defaultColumnOption;
        }
        return CONFIG_MAPPER.map(CONFIG_MAPPER_FACTORY.newConfigSource(), DbColumnOption.class);
    }
}
