package org.embulk.input.tsurugidb.getter;

import java.io.IOException;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.Record;
import com.tsurugidb.tsubakuro.sql.ResultSet;

// https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/getter/FloatColumnGetter.java
public class FloatColumnGetter extends AbstractColumnGetter {
    protected float value;

    public FloatColumnGetter(PageBuilder to, Type toType) {
        super(to, toType);
    }

    @Override
    protected void fetch(ResultSet from) throws IOException, ServerException, InterruptedException {
        value = from.fetchFloat4Value();
    }

    @Override
    protected void fetch(Record from, String name) {
        value = from.getFloat(name);
    }

    @Override
    protected Type getDefaultToType() {
        return Types.DOUBLE;
    }

    @Override
    public void booleanColumn(Column column) {
        to.setBoolean(column, value > 0.0);
    }

    @Override
    public void longColumn(Column column) {
        long l;
        try {
            l = roundFloatToLong(this.value);
        } catch (ArithmeticException e) {
            // NaN / Infinite / -Infinite
            super.longColumn(column);
            return;
        }
        to.setLong(column, l);
    }

    @Override
    public void doubleColumn(Column column) {
        to.setDouble(column, value);
    }

    @Override
    public void stringColumn(Column column) {
        to.setString(column, Float.toString(value));
    }
}
