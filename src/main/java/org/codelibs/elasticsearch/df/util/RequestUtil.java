package org.codelibs.elasticsearch.df.util;

import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestRequest;

public class RequestUtil {
    private static final TimeValue DEFAULT_SCROLL = new TimeValue(60000);

    private RequestUtil() {
    }

    public static TimeValue getScroll(final RestRequest request) {
        final String scroll = request.param("scroll");
        if (scroll != null) {
            return parseTimeValue(scroll, DEFAULT_SCROLL, "");
        } else {
            return DEFAULT_SCROLL;
        }
    }
}
