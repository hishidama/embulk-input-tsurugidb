package org.embulk.input.tsurugidb.getter;

import static java.util.Locale.ENGLISH;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.DataException;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.Record;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.sql.ResultSet;

// https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/getter/AbstractColumnGetter.java
public abstract class AbstractColumnGetter implements ColumnGetter, ColumnVisitor {
    protected static final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;

    protected final PageBuilder to;
    private final Type toType;

    public AbstractColumnGetter(PageBuilder to, Type toType) {
        this.to = to;
        this.toType = toType;
    }

    @Override
    public void getAndSet(ResultSet from, Column toColumn) {
        try {
            if (from.nextColumn()) {
                if (from.isNull()) {
                    to.setNull(toColumn);
                } else {
                    fetch(from);
                    toColumn.visit(this);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (ServerException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract void fetch(ResultSet from) throws IOException, ServerException, InterruptedException;

    @Override
    public void getAndSet(Record from, Column toColumn) {
        String name = toColumn.getName();
        if (from.isNull(name)) {
            to.setNull(toColumn);
        } else {
            fetch(from, name);
            toColumn.visit(this);
        }
    }

    protected abstract void fetch(Record from, String name);

    @Override
    public void booleanColumn(Column column) {
        to.setNull(column);
    }

    @Override
    public void longColumn(Column column) {
        to.setNull(column);
    }

    @Override
    public void doubleColumn(Column column) {
        to.setNull(column);
    }

    @Override
    public void stringColumn(Column column) {
        to.setNull(column);
    }

    @Override
    public void jsonColumn(Column column) {
        to.setNull(column);
    }

    @Override
    public void timestampColumn(Column column) {
        to.setNull(column);
    }

    @Override
    public Type getToType() {
        if (toType == null) {
            return getDefaultToType();
        }
        return toType;
    }

    protected abstract Type getDefaultToType();

    @Override
    public JsonNode encodeToJson() {
        throw new DataException(String.format(ENGLISH, "Column type '%s' set at incremental_columns option is not supported", getToType()));
    }

    @Override
    public void decodeFromJsonTo(List<Parameter> toStatement, String toIndex, JsonNode fromValue) {
        throw new DataException(String.format(ENGLISH, "Converting last_record value %s to column index %d is not supported", fromValue.toString(), toIndex));
    }

    @Override
    public void decodeFromJsonTo(RecordBuffer toRecord, String toIndex, JsonNode fromValue) {
        throw new DataException(String.format(ENGLISH, "Converting last_record value %s to column index %d is not supported", fromValue.toString(), toIndex));
    }

    final long roundDoubleToLong(final double value) {
        // TODO configurable rounding mode
        if (Math.getExponent(value) > Double.MAX_EXPONENT) {
            throw new ArithmeticException("input is infinite or NaN");
        }
        final double z = Math.rint(value);
        if (Math.abs(value - z) == 0.5) {
            return (long) (value + Math.copySign(0.5, value));
        } else {
            return (long) z;
        }
    }

    final long roundFloatToLong(final float value) {
        // TODO configurable rounding mode
        if (Math.getExponent(value) > Double.MAX_EXPONENT) {
            throw new ArithmeticException("input is infinite or NaN");
        }
        final double z = Math.rint(value);
        if (Math.abs(value - z) == 0.5) {
            return (long) (value + Math.copySign(0.5, value));
        } else {
            return (long) z;
        }
    }
}
