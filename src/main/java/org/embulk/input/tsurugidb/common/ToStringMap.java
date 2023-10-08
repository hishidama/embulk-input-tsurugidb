package org.embulk.input.tsurugidb.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.annotation.JsonCreator;

// https://github.com/embulk/embulk-input-jdbc/blob/master/embulk-input-jdbc/src/main/java/org/embulk/input/jdbc/ToStringMap.java
@SuppressWarnings("serial")
public class ToStringMap extends HashMap<String, String> {
    @JsonCreator
    ToStringMap(Map<String, ToString> map) {
        super(mapToStringString(map));
    }

    public Properties toProperties() {
        Properties props = new Properties();
        props.putAll(this);
        return props;
    }

    private static Map<String, String> mapToStringString(final Map<String, ToString> mapOfToString) {
        final HashMap<String, String> result = new HashMap<>();
        for (final Map.Entry<String, ToString> entry : mapOfToString.entrySet()) {
            final ToString value = entry.getValue();
            if (value == null) {
                result.put(entry.getKey(), "null");
            } else {
                result.put(entry.getKey(), value.toString());
            }
        }
        return result;
    }
}
