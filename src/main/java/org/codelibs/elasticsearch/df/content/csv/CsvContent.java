package org.codelibs.elasticsearch.df.content.csv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jp.sf.orangesignal.csv.CsvConfig;
import jp.sf.orangesignal.csv.CsvWriter;

import org.codelibs.elasticsearch.df.DfContentException;
import org.codelibs.elasticsearch.df.content.DataContent;
import org.codelibs.elasticsearch.df.util.MapUtil;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchHit;
import org.seasar.util.lang.StringUtil;

public class CsvContent extends DataContent {
    public CsvContent(final Client client, final RestRequest request,
            final SearchResponse response, final TimeValue keepAlive) {
        super(client, request, response, keepAlive);
    }

    @Override
    public void write(final File outputFile) {
        SearchResponse scrollResp = response;

        final CsvConfig cfg = new CsvConfig(request.param("csv.separator", ",")
                .charAt(0), request.param("csv.quote", "\"").charAt(0), request
                .param("csv.escape", "\"").charAt(0));
        cfg.setQuoteDisabled(request.paramAsBoolean("csv.quoteDisabled", false));
        cfg.setEscapeDisabled(request.paramAsBoolean("csv.escapeDisabled",
                false));
        cfg.setNullString(request.param("csv.nullString", ""));
        cfg.setIgnoreLeadingWhitespaces(request.paramAsBoolean(
                "csv.ignoreLeadingWhitespaces", true));
        cfg.setIgnoreTrailingWhitespaces(request.paramAsBoolean(
                "csv.ignoreTrailingWhitespaces", true));

        boolean appnedHeader = request.paramAsBoolean("csv.header", true);
        final String charsetName = request.param("csv.encoding", "UTF-8");
        CsvWriter csvWriter = null;
        try {
            csvWriter = new CsvWriter(new BufferedWriter(
                    new OutputStreamWriter(new FileOutputStream(outputFile),
                            charsetName)), cfg);

            final String[] fields = request.paramAsStringArray("fl",
                    StringUtil.EMPTY_STRINGS);
            final Set<String> headerSet;
            final boolean modifiableFieldSet;
            if (fields.length == 0) {
                headerSet = new LinkedHashSet<String>();
                modifiableFieldSet = true;
            } else {
                final Set<String> fieldSet = new LinkedHashSet<String>();
                for (final String field : fields) {
                    fieldSet.add(field);
                }
                headerSet = Collections.unmodifiableSet(fieldSet);
                modifiableFieldSet = false;
            }

            while (true) {
                scrollResp = client
                        .prepareSearchScroll(scrollResp.getScrollId())
                        .setScroll(keepAlive).execute().actionGet();

                for (final SearchHit hit : scrollResp.getHits()) {
                    final Map<String, Object> sourceMap = hit.sourceAsMap();
                    final Map<String, Object> dataMap = new HashMap<String, Object>();
                    MapUtil.convertToFlatMap("", sourceMap, dataMap);
                    for (final String key : dataMap.keySet()) {
                        if (modifiableFieldSet && !headerSet.contains(key)) {
                            headerSet.add(key);
                        }
                    }
                    if (appnedHeader) {
                        final List<String> headerList = new ArrayList<String>(
                                headerSet.size());
                        headerList.addAll(headerSet);
                        csvWriter.writeValues(headerList);
                        appnedHeader = false;
                    }

                    final List<String> dataList = new ArrayList<String>(
                            dataMap.size());
                    for (final String name : headerSet) {
                        final Object value = dataMap.get(name);
                        dataList.add(value != null ? value.toString() : null);
                    }
                    csvWriter.writeValues(dataList);
                }

                if (scrollResp.getHits().getHits().length == 0) {
                    break;
                }
            }
            csvWriter.flush();
        } catch (final Exception e) {
            throw new DfContentException("Failed to write data.", e);
        } finally {
            if (csvWriter != null) {
                try {
                    csvWriter.close();
                } catch (final IOException e) {
                    // ignore
                }
            }
        }
    }
}
