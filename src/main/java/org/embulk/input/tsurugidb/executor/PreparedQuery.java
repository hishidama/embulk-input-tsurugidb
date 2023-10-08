package org.embulk.input.tsurugidb.executor;

import java.util.List;

import org.embulk.input.tsurugidb.TsurugiLiteral;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

//https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/JdbcInputConnection.java
public class PreparedQuery {
    private final String tableName;
    private final String query;
    private final List<TsurugiLiteral> parameters;

    @JsonCreator
    public PreparedQuery( //
            @JsonProperty("tableName") String tableName, // for KVS
            @JsonProperty("query") String query, // for SQL
            @JsonProperty("parameters") List<TsurugiLiteral> parameters) {
        this.tableName = tableName;
        this.query = query;
        this.parameters = parameters;
    }

    @JsonProperty("tableName")
    public String getTableName() {
        return tableName;
    }

    @JsonProperty("query")
    public String getQuery() {
        return query;
    }

    @JsonProperty("parameters")
    public List<TsurugiLiteral> getParameters() {
        return parameters;
    }
}
