package org.codelibs.elasticsearch.df.util;

import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestRequest;

public class RequestUtil {
    private RequestUtil() {
    }

    public static TimeValue getScroll(final RestRequest request) {
        final String scroll = request.param("scroll");
        if (scroll != null) {
            return parseTimeValue(scroll, null);
        } else {
            return new TimeValue(60000);
        }
    }

	public static void setScroll(RestRequest request,
			SearchRequestBuilder prepareSearch) {
		if(!disableScroll(request)) {
			prepareSearch.setScroll(RequestUtil.getScroll(request));
		}
	}
	
	public static boolean disableScroll(RestRequest request) {
		return request.paramAsBoolean("from", false) || request.paramAsBoolean("size", false);
	}
}
