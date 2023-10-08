package org.embulk.input.tsurugidb.getter;

import java.io.IOException;
import java.math.BigDecimal;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.Record;
import com.tsurugidb.tsubakuro.sql.ResultSet;

// https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/getter/BigDecimalColumnGetter.java
public class DecimalColumnGetter extends AbstractColumnGetter {
    protected BigDecimal value;

    public DecimalColumnGetter(PageBuilder to, Type toType) {
        super(to, toType);
    }

    @Override
    protected void fetch(ResultSet from) throws IOException, ServerException, InterruptedException {
        value = from.fetchDecimalValue();
    }

    @Override
    protected void fetch(Record from, String name) {
        value = from.getDecimal(name);
    }

    @Override
    protected Type getDefaultToType() {
        return Types.DOUBLE;
    }

    @Override
    public void booleanColumn(Column column) {
        to.setBoolean(column, value.signum() > 0);
    }

    @Override
    public void longColumn(Column column) {
        to.setLong(column, value.longValue());
    }

    @Override
    public void doubleColumn(Column column) {
        // rounded value could be Double.NEGATIVE_INFINITY or Double.POSITIVE_INFINITY.
        double rounded = value.doubleValue();
        to.setDouble(column, rounded);
    }

    @Override
    public void stringColumn(Column column) {
        to.setString(column, value.toPlainString());
    }
}
