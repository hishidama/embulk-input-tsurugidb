package org.embulk.input.tsurugidb.getter;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.embulk.util.timestamp.TimestampFormatter;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.Record;
import com.tsurugidb.tsubakuro.sql.ResultSet;

// https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/getter/TimestampColumnGetter.java
public class DateTimeColumnGetter extends AbstractTimestampColumnGetter {
    static final String DEFAULT_FORMAT = "%Y-%m-%d %H:%M:%S";

    public DateTimeColumnGetter(PageBuilder to, Type toType, TimestampFormatter timestampFormatter, ZoneId zoneId) {
        super(to, toType, timestampFormatter, zoneId);
    }

    @Override
    protected void fetch(ResultSet from) throws IOException, ServerException, InterruptedException {
        LocalDateTime dateTime = from.fetchTimePointValue();
        setValue(dateTime);
    }

    @Override
    protected void fetch(Record from, String name) {
        LocalDateTime dateTime = from.getTimePoint(name);
        setValue(dateTime);
    }

    protected void setValue(LocalDateTime dateTime) {
        value = dateTime.toInstant(zoneOffset);
    }

    @Override
    @SuppressWarnings("deprecation")
    protected Type getDefaultToType() {
        return Types.TIMESTAMP.withFormat(DEFAULT_FORMAT);
    }
}
