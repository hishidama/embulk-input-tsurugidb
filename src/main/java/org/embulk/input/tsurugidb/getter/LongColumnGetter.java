package org.embulk.input.tsurugidb.getter;

import java.io.IOException;
import java.util.List;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

import com.fasterxml.jackson.databind.JsonNode;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.Record;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.ResultSet;

// https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/getter/LongColumnGetter.java
public class LongColumnGetter extends AbstractColumnGetter {
    protected long value;

    public LongColumnGetter(PageBuilder to, Type toType) {
        super(to, toType);
    }

    @Override
    protected void fetch(ResultSet from) throws IOException, ServerException, InterruptedException {
        value = from.fetchInt8Value();
    }

    @Override
    protected void fetch(Record from, String name) {
        value = from.getLong(name);
    }

    @Override
    protected Type getDefaultToType() {
        return Types.LONG;
    }

    @Override
    public void booleanColumn(Column column) {
        to.setBoolean(column, value > 0L);
    }

    @Override
    public void longColumn(Column column) {
        to.setLong(column, value);
    }

    @Override
    public void doubleColumn(Column column) {
        to.setDouble(column, value);
    }

    @Override
    public void stringColumn(Column column) {
        to.setString(column, Long.toString(value));
    }

    @Override
    public JsonNode encodeToJson() {
        return jsonNodeFactory.numberNode(value);
    }

    @Override
    public void decodeFromJsonTo(List<Parameter> toStatement, String toIndex, JsonNode fromValue) {
        long v = fromValue.asLong();
        var parameter = Parameters.of(toIndex, v);
        toStatement.add(parameter);
    }

    @Override
    public void decodeFromJsonTo(RecordBuffer toRecord, String toIndex, JsonNode fromValue) {
        long v = fromValue.asLong();
        toRecord.add(toIndex, v);
    }
}
