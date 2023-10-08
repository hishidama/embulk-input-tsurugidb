package org.embulk.input.tsurugidb;

import java.util.List;
import java.util.Optional;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;

// https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/JdbcSchema.java
public class TsurugiQuerySchema {
	private List<TsurugiColumn> columns;

	@JsonCreator
	public TsurugiQuerySchema(List<TsurugiColumn> columns) {
		this.columns = columns;
	}

	@JsonValue
	public List<TsurugiColumn> getColumns() {
		return columns;
	}

	public int getCount() {
		return columns.size();
	}

	public TsurugiColumn getColumn(int i) {
		return columns.get(i);
	}

	public Optional<TsurugiColumn> findColumn(String caseInsensitiveName) {
		// find by case sensitive first
		for (var column : columns) {
			if (column.getName().equals(caseInsensitiveName)) {
				return Optional.of(column);
			}
		}
		// find by case insensitive
		for (var column : columns) {
			if (column.getName().equalsIgnoreCase(caseInsensitiveName)) {
				return Optional.of(column);
			}
		}
		return Optional.empty();
	}

	public AtomType getColumnType(String caseInsensitiveName) {
		return findColumn(caseInsensitiveName).map(c -> c.getSqlType()).orElse(null);
	}
}
