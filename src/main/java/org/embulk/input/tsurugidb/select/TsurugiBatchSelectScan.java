package org.embulk.input.tsurugidb.select;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.embulk.input.tsurugidb.executor.TsurugiKvsExecutor;
import org.embulk.input.tsurugidb.getter.ColumnGetter;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.slf4j.Logger;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.RecordCursor;

public class TsurugiBatchSelectScan implements BatchSelect {

    private final TsurugiKvsExecutor kvsExecutor;
    private final String tableName;
    private final RecordBuffer lowerKey;
    private boolean fetched = false;
    private RecordCursor cursor;

    public TsurugiBatchSelectScan(TsurugiKvsExecutor kvsExecutor, String tableName, RecordBuffer lowerKey) {
        this.kvsExecutor = kvsExecutor;
        this.tableName = tableName;
        this.lowerKey = lowerKey;
    }

    @Override
    public long fetch(Map<String, ColumnGetter> getters, PageBuilder pageBuilder, Logger logger) throws ServerException {
        RecordCursor cursor = fetch(logger);
        if (cursor == null) {
            return 0;
        }

        Collection<ColumnGetter> gettersList = getters.values();
        List<Column> columns = pageBuilder.getSchema().getColumns();
        long rows = 0;
        long reportRows = 500;
        try {
            while (cursor.next()) {
                var record = cursor.getRecord();
                int i = 0;
                for (ColumnGetter getter : gettersList) {
                    getter.getAndSet(record, columns.get(i));
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

    protected RecordCursor fetch(Logger logger) throws ServerException {
        if (fetched) {
            return null;
        }

        long startTime = System.currentTimeMillis();

        try {
            cursor = kvsExecutor.executeScan(tableName, lowerKey);

            double seconds = (System.currentTimeMillis() - startTime) / 1000.0;
            logger.info(String.format("> %.2f seconds", seconds));
            fetched = true;
            return cursor;
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws ServerException {
        try (var c = cursor) {
            // close only
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
