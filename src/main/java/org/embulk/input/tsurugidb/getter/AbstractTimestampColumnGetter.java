package org.embulk.input.tsurugidb.getter;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;
import org.embulk.util.timestamp.TimestampFormatter;

// https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/getter/AbstractTimestampColumnGetter.java
public abstract class AbstractTimestampColumnGetter extends AbstractColumnGetter {
	protected final TimestampFormatter timestampFormatter;
	protected final ZoneOffset zoneOffset;
	protected Instant value;

	public AbstractTimestampColumnGetter(PageBuilder to, Type toType, TimestampFormatter timestampFormatter,
			ZoneId zoneId) {
		super(to, toType);

		this.timestampFormatter = timestampFormatter;
		this.zoneOffset = zoneId.getRules().getOffset(Instant.now());
	}

	@Override
	public void stringColumn(Column column) {
		to.setString(column, timestampFormatter.format(value));
	}

	@Override
	public void jsonColumn(Column column) {
		throw new UnsupportedOperationException(
				"This plugin doesn't support json type. Please try to upgrade version of the plugin using 'embulk gem update' command. If the latest version still doesn't support json type, please contact plugin developers, or change configuration of input plugin not to use json type.");
	}

	@Override
	public void timestampColumn(Column column) {
		to.setTimestamp(column, value);
	}
}
