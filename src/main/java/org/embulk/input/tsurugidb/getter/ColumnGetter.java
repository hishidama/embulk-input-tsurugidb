package org.embulk.input.tsurugidb.getter;

import java.util.List;

import org.embulk.spi.Column;
import org.embulk.spi.type.Type;

import com.fasterxml.jackson.databind.JsonNode;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.tsubakuro.kvs.Record;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.sql.ResultSet;

// https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/getter/ColumnGetter.java
public interface ColumnGetter {

    public void getAndSet(ResultSet from, Column toColumn);

    public void getAndSet(Record from, Column toColumn);

    public Type getToType();

    public JsonNode encodeToJson();

    public void decodeFromJsonTo(List<Parameter> toStatement, String toIndex, JsonNode fromValue);

    public void decodeFromJsonTo(RecordBuffer toRecord, String toIndex, JsonNode fromValue);
}
