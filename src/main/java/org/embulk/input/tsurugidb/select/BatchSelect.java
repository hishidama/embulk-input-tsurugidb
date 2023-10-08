package org.embulk.input.tsurugidb.select;

import java.util.Map;

import org.embulk.input.tsurugidb.getter.ColumnGetter;
import org.embulk.spi.PageBuilder;
import org.slf4j.Logger;

import com.tsurugidb.tsubakuro.exception.ServerException;

//https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/JdbcInputConnection.java
public interface BatchSelect extends AutoCloseable {
    public long fetch(Map<String, ColumnGetter> getters, PageBuilder pageBuilder, Logger logger) throws ServerException;

    @Override
    public void close() throws ServerException;
}
