package org.embulk.input.tsurugidb.getter;

import static java.util.Locale.ENGLISH;

import java.time.ZoneId;

import org.embulk.config.ConfigException;
import org.embulk.input.tsurugidb.TsurugiColumn;
import org.embulk.input.tsurugidb.common.DbColumnOption;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.embulk.util.timestamp.TimestampFormatter;

import com.tsurugidb.sql.proto.SqlCommon.AtomType;

public class ColumnGetterFactory {
    protected final PageBuilder to;
    private final ZoneId defaultTimeZone;

    public ColumnGetterFactory(PageBuilder to, ZoneId defaultTimeZone) {
        this.to = to;
        this.defaultTimeZone = defaultTimeZone;
    }

    public ColumnGetter newColumnGetter(TsurugiColumn column, DbColumnOption option, AtomType valueType) {
        Type toType = getToType(option);
        switch (valueType) {
        case BOOLEAN:
            return new BooleanColumnGetter(to, toType);
        case INT4:
            return new IntColumnGetter(to, toType);
        case INT8:
            return new LongColumnGetter(to, toType);
        case FLOAT4:
            return new FloatColumnGetter(to, toType);
        case FLOAT8:
            return new DoubleColumnGetter(to, toType);
        case DECIMAL:
            return new DecimalColumnGetter(to, toType);
        case CHARACTER:
            return new StringColumnGetter(to, toType);
        case DATE:
            return new DateColumnGetter(to, toType, newTimestampFormatter(option, DateColumnGetter.DEFAULT_FORMAT), getTimeZone(option));
        case TIME_OF_DAY:
            return new TimeColumnGetter(to, toType, newTimestampFormatter(option, TimeColumnGetter.DEFAULT_FORMAT), getTimeZone(option));
        case TIME_POINT:
            return new DateTimeColumnGetter(to, toType, newTimestampFormatter(option, DateTimeColumnGetter.DEFAULT_FORMAT), getTimeZone(option));
        case TIME_OF_DAY_WITH_TIME_ZONE:
            return new OffsetTimeColumnGetter(to, toType, newTimestampFormatter(option, OffsetTimeColumnGetter.DEFAULT_FORMAT), getTimeZone(option));
        case TIME_POINT_WITH_TIME_ZONE:
            return new OffsetDateTimeColumnGetter(to, toType, newTimestampFormatter(option, OffsetDateTimeColumnGetter.DEFAULT_FORMAT), getTimeZone(option));
        default:
            throw new ConfigException(String.format(ENGLISH, "Unknown value_type '%s' for column '%s'", valueType, column.getName()));
        }
    }

    @SuppressWarnings("deprecation")
    protected Type getToType(DbColumnOption option) {
        if (!option.getType().isPresent()) {
            return null;
        }
        Type toType = option.getType().get();
        if (toType instanceof TimestampType && option.getTimestampFormat().isPresent()) {
            toType = ((TimestampType) toType).withFormat(option.getTimestampFormat().get());
        }
        return toType;
    }

    private TimestampFormatter newTimestampFormatter(DbColumnOption option, String defaultTimestampFormat) {
        final String format = option.getTimestampFormat().orElse(defaultTimestampFormat);
        final ZoneId timezone = getTimeZone(option);
        return TimestampFormatter.builder(format, true).setDefaultZoneId(timezone).build();
    }

    protected ZoneId getTimeZone(DbColumnOption option) {
        return option.getTimeZone().orElse(this.defaultTimeZone);
    }

    // @see java.sql.Types
    public String getJdbcType(TsurugiColumn column) {
        AtomType sqlType = column.getSqlType();
        switch (sqlType) {
        case BOOLEAN:
            return "boolean";
        case INT4:
        case INT8:
            return "long";
        case FLOAT4:
            return "float";
        case FLOAT8:
            return "double";
        case DECIMAL:
            return "decimal";
        case CHARACTER:
            return "string";
        case DATE:
            return "date";
        case TIME_OF_DAY:
        case TIME_OF_DAY_WITH_TIME_ZONE:
            return "time";
        case TIME_POINT:
        case TIME_POINT_WITH_TIME_ZONE:
            return "timestamp";
        default:
            throw new UnsupportedOperationException(String.format(ENGLISH, "Unknown value_type '%s' for column '%s'", sqlType, column.getName()));
        }
    }
}
