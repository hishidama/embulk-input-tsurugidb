package org.embulk.input.tsurugidb.getter;

import java.io.IOException;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.Record;
import com.tsurugidb.tsubakuro.sql.ResultSet;

// https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/getter/BooleanColumnGetter.java
public class BooleanColumnGetter extends AbstractColumnGetter {
    protected boolean value;

    public BooleanColumnGetter(PageBuilder to, Type toType) {
        super(to, toType);
    }

    @Override
    protected void fetch(ResultSet from) throws IOException, ServerException, InterruptedException {
        value = from.fetchBooleanValue();
    }

    @Override
    protected void fetch(Record from, String name) {
        value = from.getBoolean(name);
    }

    @Override
    protected Type getDefaultToType() {
        return Types.BOOLEAN;
    }

    @Override
    public void booleanColumn(Column column) {
        to.setBoolean(column, value);
    }

    @Override
    public void longColumn(Column column) {
        to.setLong(column, value ? 1L : 0L);
    }

    @Override
    public void doubleColumn(Column column) {
        to.setDouble(column, value ? 1.0 : 0.0);
    }

    @Override
    public void stringColumn(Column column) {
        to.setString(column, Boolean.toString(value));
    }
}
