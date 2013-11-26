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
import org.codelibs.elasticsearch.df.util.RequestUtil;
import org.codelibs.elasticsearch.util.NettyUtils;
import org.codelibs.elasticsearch.util.StringUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.netty.channel.Channel;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

public class CsvContent extends DataContent {
    private static final ESLogger logger = Loggers.getLogger(CsvContent.class);
    private final String charsetName;
    private final CsvConfig csvConfig;
    private boolean appnedHeader;
    private Set<String> headerSet;
    private boolean modifiableFieldSet;
    private final Channel nettyChannel;

    public CsvContent(final Client client, final RestRequest request,
            final RestChannel channel) {
        super(client, request);
        csvConfig = new CsvConfig(
                request.param("csv.separator", ",").charAt(0), request.param(
                        "csv.quote", "\"").charAt(0), request.param(
                        "csv.escape", "\"").charAt(0));
        csvConfig.setQuoteDisabled(request.paramAsBoolean("csv.quoteDisabled",
                false));
        csvConfig.setEscapeDisabled(request.paramAsBoolean(
                "csv.escapeDisabled", false));
        csvConfig.setNullString(request.param("csv.nullString", ""));
        csvConfig.setIgnoreLeadingWhitespaces(request.paramAsBoolean(
                "csv.ignoreLeadingWhitespaces", true));
        csvConfig.setIgnoreTrailingWhitespaces(request.paramAsBoolean(
                "csv.ignoreTrailingWhitespaces", true));

        appnedHeader = request.paramAsBoolean("csv.header", true);
        charsetName = request.param("csv.encoding", "UTF-8");

        final String[] fields = request.paramAsStringArray("fl",
                StringUtils.EMPTY_STRINGS);
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

        nettyChannel = NettyUtils.getChannel(channel);

        if (logger.isDebugEnabled()) {
            logger.debug("CsvConfig: " + csvConfig + ", appnedHeader: "
                    + appnedHeader + ", charsetName: " + charsetName
                    + ", headerSet: " + headerSet + ", nettyChannel: "
                    + nettyChannel);
        }
    }

    @Override
    public void write(final File outputFile, final SearchResponse response,
            final ActionListener<Void> listener) {
        try {
            final OnLoadListener onLoadListener = new OnLoadListener(
                    outputFile, listener);
            onLoadListener.onResponse(response);
        } catch (final Exception e) {
            listener.onFailure(new DfContentException("Failed to write data.",
                    e));
        }
    }

    protected class OnLoadListener implements ActionListener<SearchResponse> {
        protected ActionListener<Void> listener;

        protected CsvWriter csvWriter;
        protected File outputFile;

        private long currentCount = 0;

        protected OnLoadListener(final File outputFile,
                final ActionListener<Void> listener) {
            this.outputFile = outputFile;
            this.listener = listener;
            try {
                csvWriter = new CsvWriter(
                        new BufferedWriter(new OutputStreamWriter(
                                new FileOutputStream(outputFile), charsetName)),
                        csvConfig);
            } catch (final Exception e) {
                throw new DfContentException("Could not open "
                        + outputFile.getAbsolutePath(), e);
            }
        }

        @Override
        public void onResponse(final SearchResponse response) {
            if (!isConnected()) {
                onFailure(new DfContentException("Disconnected."));
                return;
            }

            final SearchHits hits = response.getHits();

            final int size = hits.getHits().length;
            currentCount += size;
            logger.info("scrollId: " + response.getScrollId() + ", totalHits: "
                    + hits.totalHits() + ", hits: " + size + ", current: "
                    + currentCount);

            try {
                for (final SearchHit hit : hits) {
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

                if (size == 0) {
                    // end
                    csvWriter.flush();
                    close();
                    listener.onResponse(null);
                } else {
                    client.prepareSearchScroll(response.getScrollId())
                            .setScroll(RequestUtil.getScroll(request))
                            .execute(this);
                }
            } catch (final Exception e) {
                onFailure(e);
            }
        }

        private boolean isConnected() {
            return nettyChannel != null && nettyChannel.isConnected();
        }

        @Override
        public void onFailure(final Throwable e) {
            try {
                close();
            } catch (final Exception e1) {
                // ignore
            }
            listener.onFailure(new DfContentException("Failed to write data.",
                    e));
        }

        private void close() {
            if (csvWriter != null) {
                try {
                    csvWriter.close();
                } catch (final IOException e) {
                    throw new DfContentException("Could not close "
                            + outputFile.getAbsolutePath(), e);
                }
            }
        }
    }
}
