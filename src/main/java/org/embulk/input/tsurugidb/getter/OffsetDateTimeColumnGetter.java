package org.embulk.input.tsurugidb.getter;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneId;

import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.embulk.util.timestamp.TimestampFormatter;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.Record;
import com.tsurugidb.tsubakuro.sql.ResultSet;

// https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/getter/TimestampColumnGetter.java
public class OffsetDateTimeColumnGetter extends AbstractTimestampColumnGetter {
    static final String DEFAULT_FORMAT = "%Y-%m-%d %H:%M:%S";

    public OffsetDateTimeColumnGetter(PageBuilder to, Type toType, TimestampFormatter timestampFormatter, ZoneId zoneId) {
        super(to, toType, timestampFormatter, zoneId);
    }

    @Override
    protected void fetch(ResultSet from) throws IOException, ServerException, InterruptedException {
        OffsetDateTime dateTime = from.fetchTimePointWithTimeZoneValue();
        setValue(dateTime);
    }

    @Override
    protected void fetch(Record from, String name) {
        OffsetDateTime dateTime = from.getTimePointWithTimeZone(name);
        setValue(dateTime);
    }

    protected void setValue(OffsetDateTime dateTime) {
        value = dateTime.toInstant();
    }

    @Override
    @SuppressWarnings("deprecation")
    protected Type getDefaultToType() {
        return Types.TIMESTAMP.withFormat(DEFAULT_FORMAT);
    }
}
