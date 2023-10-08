package org.embulk.input.tsurugidb.getter;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;

import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.embulk.util.timestamp.TimestampFormatter;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.Record;
import com.tsurugidb.tsubakuro.sql.ResultSet;

// https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/getter/DateColumnGetter.java
public class DateColumnGetter extends AbstractTimestampColumnGetter {
    static final String DEFAULT_FORMAT = "%Y-%m-%d";

    public DateColumnGetter(PageBuilder to, Type toType, TimestampFormatter timestampFormatter, ZoneId zoneId) {
        super(to, toType, timestampFormatter, zoneId);
    }

    @Override
    protected void fetch(ResultSet from) throws IOException, ServerException, InterruptedException {
        LocalDate date = from.fetchDateValue();
        setValue(date);
    }

    @Override
    protected void fetch(Record from, String name) {
        LocalDate date = from.getDate(name);
        setValue(date);
    }

    protected void setValue(LocalDate date) {
        long epochSecond = date.toEpochSecond(LocalTime.MIN, zoneOffset);
        value = Instant.ofEpochSecond(epochSecond);
    }

    @Override
    @SuppressWarnings("deprecation")
    protected Type getDefaultToType() {
        return Types.TIMESTAMP.withFormat(DEFAULT_FORMAT);
    }
}
