package org.embulk.input.tsurugidb.select;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.embulk.input.tsurugidb.executor.TsurugiSqlExecutor;
import org.embulk.input.tsurugidb.getter.ColumnGetter;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.slf4j.Logger;

import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.ResultSet;

//https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/JdbcInputConnection.java
public class TsurugiSingleSelect implements BatchSelect {

    private final TsurugiSqlExecutor sqlExecutor;
    private final PreparedStatement fetchStatement;
    private final List<Parameter> parameters;
    private boolean fetched = false;
    private ResultSet resultSet;

    public TsurugiSingleSelect(TsurugiSqlExecutor sqlExecutor, PreparedStatement fetchStatement, List<Parameter> parameters) {
        this.sqlExecutor = sqlExecutor;
        this.fetchStatement = fetchStatement;
        this.parameters = parameters;
    }

    // https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/AbstractJdbcInputPlugin.java
    @Override
    public long fetch(Map<String, ColumnGetter> getters, PageBuilder pageBuilder, Logger logger) throws ServerException {
        ResultSet result = fetch(logger);
        if (result == null) {
            return 0;
        }

        Collection<ColumnGetter> gettersList = getters.values();
        List<Column> columns = pageBuilder.getSchema().getColumns();
        long rows = 0;
        long reportRows = 500;
        try {
            while (result.nextRow()) {
                int i = 0;
                for (ColumnGetter getter : gettersList) {
                    getter.getAndSet(result, columns.get(i));
                    i++;
                }
                pageBuilder.addRecord();
                rows++;
                if (rows % reportRows == 0) {
                    logger.info(String.format("Fetched %,d rows.", rows));
                    reportRows *= 2;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return rows;
    }

    protected ResultSet fetch(Logger logger) throws ServerException {
        if (fetched) {
            return null;
        }

        long startTime = System.currentTimeMillis();

        try {
            resultSet = sqlExecutor.executeQuery(fetchStatement, parameters);

            double seconds = (System.currentTimeMillis() - startTime) / 1000.0;
            logger.info(String.format("> %.2f seconds", seconds));
            fetched = true;
            return resultSet;
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws ServerException {
        try (fetchStatement; var r = resultSet) {
            // close only
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
