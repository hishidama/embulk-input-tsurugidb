package org.embulk.input.tsurugidb.getter;

import java.io.IOException;
import java.util.List;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.embulk.util.json.JsonParseException;
import org.embulk.util.json.JsonParser;
import org.msgpack.value.Value;

import com.fasterxml.jackson.databind.JsonNode;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.Record;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.ResultSet;

// https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/getter/StringColumnGetter.java
public class StringColumnGetter extends AbstractColumnGetter {
    protected final JsonParser jsonParser = new JsonParser();

    protected String value;

    public StringColumnGetter(PageBuilder to, Type toType) {
        super(to, toType);
    }

    @Override
    protected void fetch(ResultSet from) throws IOException, ServerException, InterruptedException {
        value = from.fetchCharacterValue();
    }

    @Override
    protected void fetch(Record from, String name) {
        value = from.getCharacter(name);
    }

    @Override
    protected Type getDefaultToType() {
        return Types.STRING;
    }

    @Override
    public void longColumn(Column column) {
        long l;
        try {
            l = Long.parseLong(value);
        } catch (NumberFormatException e) {
            super.longColumn(column);
            return;
        }
        to.setLong(column, l);
    }

    @Override
    public void doubleColumn(Column column) {
        double d;
        try {
            d = Double.parseDouble(value);
        } catch (NumberFormatException e) {
            super.doubleColumn(column);
            return;
        }
        to.setDouble(column, d);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void jsonColumn(Column column) {
        Value v;
        try {
            v = jsonParser.parse(value);
        } catch (JsonParseException e) {
            super.jsonColumn(column);
            return;
        }
        to.setJson(column, v);
    }

    @Override
    public void stringColumn(Column column) {
        to.setString(column, value);
    }

    @Override
    public JsonNode encodeToJson() {
        return jsonNodeFactory.textNode(value);
    }

    @Override
    public void decodeFromJsonTo(List<Parameter> toStatement, String toIndex, JsonNode fromValue) {
        String v = fromValue.asText();
        var parameter = Parameters.of(toIndex, v);
        toStatement.add(parameter);
    }

    @Override
    public void decodeFromJsonTo(RecordBuffer toRecord, String toIndex, JsonNode fromValue) {
        String v = fromValue.asText();
        toRecord.add(toIndex, v);
    }
}
