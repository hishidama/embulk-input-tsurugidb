package org.embulk.input.tsurugidb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;

// https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/JdbcColumn.java
public class TsurugiColumn {

	private String name;
	private String typeName;
	private AtomType sqlType;
	private int precision;
	private int scale;

	@JsonCreator
	public TsurugiColumn( //
			@JsonProperty("name") String name, //
			@JsonProperty("typeName") String typeName, //
			@JsonProperty("sqlType") AtomType sqlType, //
			@JsonProperty("precision") int precision, //
			@JsonProperty("scale") int scale) {
		this.name = name;
		this.typeName = typeName;
		this.sqlType = sqlType;
		this.precision = precision;
		this.scale = scale;
	}

	@JsonProperty("name")
	public String getName() {
		return name;
	}

	@JsonProperty("typeName")
	public String getTypeName() {
		return typeName;
	}

	@JsonProperty("sqlType")
	public AtomType getSqlType() {
		return sqlType;
	}

	@JsonProperty("precision")
	public int getPrecision() {
		return precision;
	}

	@JsonProperty("scale")
	public int getScale() {
		return scale;
	}
}
