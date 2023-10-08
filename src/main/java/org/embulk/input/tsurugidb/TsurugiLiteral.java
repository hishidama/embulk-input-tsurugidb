package org.embulk.input.tsurugidb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;

// https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/JdbcLiteral.java
public class TsurugiLiteral {

	private final String columnName;
	private final AtomType type;
	private final JsonNode value;

	@JsonCreator
	public TsurugiLiteral( //
			@JsonProperty("columnName") String columnName, //
			@JsonProperty("type") AtomType type, //
			@JsonProperty("value") JsonNode value) {
		this.columnName = columnName;
		this.type = type;
		this.value = value;
	}

	@JsonProperty("columnName")
	public String getColumnName() {
		return columnName;
	}

	@JsonProperty("type")
	public AtomType getType() {
		return type;
	}

	@JsonProperty("value")
	public JsonNode getValue() {
		return value;
	}

	@Override
	public String toString() {
		return value.toString();
	}
}
