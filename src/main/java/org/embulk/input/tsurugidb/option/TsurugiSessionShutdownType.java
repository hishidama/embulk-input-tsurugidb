package org.embulk.input.tsurugidb.option;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

import org.embulk.config.ConfigException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.tsurugidb.tsubakuro.common.ShutdownType;

public enum TsurugiSessionShutdownType {
    NOTHING(null), //
    GRACEFUL(ShutdownType.GRACEFUL), //
    FORCEFUL(ShutdownType.FORCEFUL);

    private final ShutdownType shutdownType;

    private TsurugiSessionShutdownType(ShutdownType shutdownType) {
        this.shutdownType = shutdownType;
    }

    public ShutdownType toShutdownType() {
        return this.shutdownType;
    }

    @JsonValue
    @Override
    public String toString() {
        return name().toLowerCase(Locale.ENGLISH);
    }

    @JsonCreator
    public static TsurugiSessionShutdownType fromString(String value) {
        try {
            return valueOf(value.toUpperCase(Locale.ENGLISH));
        } catch (Exception e) {
            var ce = new ConfigException(MessageFormat.format("Unknown session_shutdown_type ''{0}''. Supported session_shutdown_type are {1}", //
                    value, Arrays.stream(values()).map(TsurugiSessionShutdownType::toString).map(String::toLowerCase).collect(Collectors.joining(", "))));
            ce.addSuppressed(e);
            throw ce;
        }
    }
}
