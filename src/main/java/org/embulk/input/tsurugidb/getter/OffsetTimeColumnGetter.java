package org.embulk.input.tsurugidb.getter;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetTime;
import java.time.ZoneId;

import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.embulk.util.timestamp.TimestampFormatter;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.Record;
import com.tsurugidb.tsubakuro.sql.ResultSet;

// https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/getter/TimeColumnGetter.java
public class OffsetTimeColumnGetter extends AbstractTimestampColumnGetter {
    static final String DEFAULT_FORMAT = "%H:%M:%S";

    public OffsetTimeColumnGetter(PageBuilder to, Type toType, TimestampFormatter timestampFormatter, ZoneId zoneId) {
        super(to, toType, timestampFormatter, zoneId);
    }

    @Override
    protected void fetch(ResultSet from) throws IOException, ServerException, InterruptedException {
        OffsetTime time = from.fetchTimeOfDayWithTimeZoneValue();
        setValue(time);
    }

    @Override
    protected void fetch(Record from, String name) {
        OffsetTime time = from.getTimeOfDayWithTimeZone(name);
        setValue(time);
    }

    protected void setValue(OffsetTime time) {
        long epochSecond = time.toEpochSecond(LocalDate.EPOCH);
        value = Instant.ofEpochSecond(epochSecond, time.getNano());
    }

    @Override
    @SuppressWarnings("deprecation")
    protected Type getDefaultToType() {
        return Types.TIMESTAMP.withFormat(DEFAULT_FORMAT);
    }
}
