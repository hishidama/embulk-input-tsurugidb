package org.embulk.input.tsurugidb.getter;

import java.io.IOException;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.embulk.util.json.JsonParseException;
import org.embulk.util.json.JsonParser;
import org.msgpack.value.Value;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.Record;
import com.tsurugidb.tsubakuro.sql.ResultSet;

// https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/getter/JsonColumnGetter.java
public class JsonColumnGetter extends AbstractColumnGetter {
    protected final JsonParser jsonParser = new JsonParser();

    protected String value;

    public JsonColumnGetter(PageBuilder to, Type toType) {
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
        return Types.JSON;
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
}
