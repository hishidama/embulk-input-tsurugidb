package org.embulk.input.tsurugidb;

import java.util.OptionalInt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tsurugidb.sql.proto.SqlCommon;
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

    public static String getTypeName(SqlCommon.Column column) {
        AtomType atomType = column.getAtomType();
        switch (atomType) {
        case INT4:
            return "INT";
        case INT8:
            return "BIGINT";
        case FLOAT4:
            return "REAL";
        case FLOAT8:
            return "DOUBLE";
        case DECIMAL:
            return getTypeNameDecimal(column);
        case CHARACTER:
            return getTypeNameVarLength("CHAR", column);
        case OCTET:
            return getTypeNameVarLength("BINARY", column);
        case DATE:
            return "DATE";
        case TIME_OF_DAY:
            return "TIME";
        case TIME_POINT:
            return "TIMESTAMP";
        case TIME_OF_DAY_WITH_TIME_ZONE:
            return "TIME WITH TIME ZONE";
        case TIME_POINT_WITH_TIME_ZONE:
            return "TIMESTAMP WITH TIME ZONE";
        default:
            return atomType.name();
        }
    }

    private static String getTypeNameDecimal(SqlCommon.Column column) {
        var sb = new StringBuilder("DECIMAL");

        var precision = getPrecision(column);
        if (precision != null) {
            sb.append("(");
            if (precision.isPresent()) {
                sb.append(precision.getAsInt());
            } else {
                sb.append("*");
            }

            var scale = getScale(column);
            if (scale != null) {
                sb.append(", ");
                if (scale.isPresent()) {
                    sb.append(scale.getAsInt());
                } else {
                    sb.append("*");
                }
            }

            sb.append(")");
        }

        return sb.toString();
    }

    public static int getPrecisionAsInt(SqlCommon.Column column) {
        var precision = getPrecision(column);
        if (precision == null) {
            return 38;
        }
        return precision.orElse(38);
    }

    private static OptionalInt getPrecision(SqlCommon.Column column) {
        var precision = column.getPrecisionOptCase();
        if (precision == null) {
            return null;
        }
        switch (precision) {
        case PRECISION:
            return OptionalInt.of(column.getPrecision());
        case ARBITRARY_PRECISION:
            return OptionalInt.empty();
        case PRECISIONOPT_NOT_SET:
        default:
            return null;
        }
    }

    public static int getScaleAsInt(SqlCommon.Column column) {
        var scale = getScale(column);
        if (scale == null) {
            return 0;
        }
        return scale.orElse(0);
    }

    private static OptionalInt getScale(SqlCommon.Column column) {
        var scale = column.getScaleOptCase();
        if (scale == null) {
            return null;
        }
        switch (scale) {
        case SCALE:
            return OptionalInt.of(column.getScale());
        case ARBITRARY_SCALE:
            return OptionalInt.empty();
        case SCALEOPT_NOT_SET:
        default:
            return null;
        }
    }

    private static String getTypeNameVarLength(String baseName, SqlCommon.Column column) {
        var varying = column.getVaryingOptCase();
        if (varying == null) {
            return column.getAtomType().name();
        }

        String typeName;
        switch (varying) {
        case VARYINGOPT_NOT_SET:
            typeName = baseName;
            break;
        case VARYING:
            typeName = "VAR" + baseName;
            break;
        default:
            return column.getAtomType().name();
        }

        var length = column.getLengthOptCase();
        if (length == null) {
            return typeName;
        }
        switch (length) {
        case LENGTH:
            return typeName + "(" + column.getLength() + ")";
        case ARBITRARY_LENGTH:
            return typeName + "(*)";
        case LENGTHOPT_NOT_SET:
        default:
            return typeName;
        }
    }
}
