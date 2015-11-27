package org.codelibs.elasticsearch.df.util;

import java.util.Map;

public class MapUtils {
    private MapUtils() {
    }

    @SuppressWarnings("unchecked")
    public static void convertToFlatMap(final String prefix,
            final Map<String, Object> oldMap, final Map<String, Object> newMap) {
        for (final Map.Entry<String, Object> entry : oldMap.entrySet()) {
            final Object value = entry.getValue();
            if (value instanceof Map) {
                convertToFlatMap(prefix + entry.getKey() + ".",
                        (Map<String, Object>) value, newMap);
            } else {
                newMap.put(prefix + entry.getKey(), value);
            }
        }
    }
}
