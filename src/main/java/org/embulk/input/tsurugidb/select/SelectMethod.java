package org.embulk.input.tsurugidb.select;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

import org.embulk.config.ConfigException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum SelectMethod {
    SELECT, SCAN;

    @JsonValue
    @Override
    public String toString() {
        return name().toLowerCase(Locale.ENGLISH);
    }

    @JsonCreator
    public static SelectMethod fromString(String value) {
        try {
            return valueOf(value.toUpperCase(Locale.ENGLISH));
        } catch (Exception e) {
            var ce = new ConfigException(MessageFormat.format("Unknown insert_method ''{0}''. Supported modes are {1}", //
                    value, Arrays.stream(values()).map(SelectMethod::toString).collect(Collectors.joining(", "))));
            ce.addSuppressed(e);
            throw ce;
        }
    }
}
