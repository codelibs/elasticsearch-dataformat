package org.codelibs.elasticsearch.df.content.csv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;
import org.codelibs.elasticsearch.df.content.ContentType;
import org.codelibs.elasticsearch.df.content.DataContent;
import org.codelibs.elasticsearch.df.util.MapUtils;
import org.codelibs.elasticsearch.df.util.RequestUtil;
import org.codelibs.elasticsearch.df.util.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import com.orangesignal.csv.CsvConfig;
import com.orangesignal.csv.CsvWriter;

public class CsvContent extends DataContent {
    private static final Logger logger = Loggers.getLogger(CsvContent.class);

    private final String charsetName;

    private final CsvConfig csvConfig;

    private final boolean appendHeader;

    private Set<String> headerSet;

    private boolean modifiableFieldSet;

    public CsvContent(final Client client, final RestRequest request, final ContentType contentType) {
        super(client, request, contentType);
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

        appendHeader = request.paramAsBoolean("append.header", true);
        charsetName = request.param("csv.encoding", "UTF-8");

        String fields_name = "fields_name";
        if (request.hasParam("fl")) {
            fields_name = "fl";
        }
        final String[] fields = request.paramAsStringArray(fields_name,
                StringUtils.EMPTY_STRINGS);
        if (fields.length == 0) {
            headerSet = new LinkedHashSet<>();
            modifiableFieldSet = true;
        } else {
            final Set<String> fieldSet = new LinkedHashSet<>();
            for (final String field : fields) {
                fieldSet.add(field.trim());
            }
            headerSet = Collections.unmodifiableSet(fieldSet);
            modifiableFieldSet = false;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("CsvConfig: " + csvConfig + ", appendHeader: "
                    + appendHeader + ", charsetName: " + charsetName
                    + ", headerSet: " + headerSet);
        }
    }

    @Override
    public void write(final File outputFile, final SearchResponse response, final RestChannel channel,
                      final ActionListener<Void> listener) {
        try {
            final OnLoadListener onLoadListener = new OnLoadListener(
                    outputFile, listener);
            onLoadListener.onResponse(response);
        } catch (final Exception e) {
            listener.onFailure(new ElasticsearchException("Failed to write data.",
                    e));
        }
    }

    protected class OnLoadListener implements ActionListener<SearchResponse> {
        protected ActionListener<Void> listener;

        protected CsvWriter csvWriter;

        protected File outputFile;

        private long currentCount = 0;

        protected OnLoadListener(final File outputFile, final ActionListener<Void> listener) {
            this.outputFile = outputFile;
            this.listener = listener;
            try {
                csvWriter = new CsvWriter(
                        new BufferedWriter(new OutputStreamWriter(
                                new FileOutputStream(outputFile), charsetName)),
                        csvConfig);
            } catch (final Exception e) {
                throw new ElasticsearchException("Could not open "
                        + outputFile.getAbsolutePath(), e);
            }
        }

        @Override
        public void onResponse(final SearchResponse response) {
            final String scrollId = response.getScrollId();
            final SearchHits hits = response.getHits();
            final int size = hits.getHits().length;
            currentCount += size;
            if (logger.isDebugEnabled()) {
                logger.debug("scrollId: " + scrollId + ", totalHits: "
                        + hits.getTotalHits() + ", hits: " + size + ", current: "
                        + currentCount);
            }
            try {
                for (final SearchHit hit : hits) {
                    final Map<String, Object> sourceMap = hit.getSourceAsMap();
                    final Map<String, Object> dataMap = new HashMap<>();
                    MapUtils.convertToFlatMap("", sourceMap, dataMap);
                    for (final String key : dataMap.keySet()) {
                        if (modifiableFieldSet && !headerSet.contains(key)) {
                            headerSet.add(key);
                        }
                    }
                    final List<String> dataList = new ArrayList<>(
                            dataMap.size());
                    for (final String name : headerSet) {
                        final Object value = dataMap.get(name);
                        dataList.add(value != null ? value.toString() : null);
                    }
                    csvWriter.writeValues(dataList);
                }

                if (size == 0 || scrollId == null) {
                    // end
                    csvWriter.flush();
                    close();
                    if (appendHeader) {
                        boolean finished = false;
                        final Path tempFile = Files
                                .createTempFile("dataformat_", ".csv");
                        try (final OutputStream out = Files
                                .newOutputStream(tempFile);
                             final CsvWriter writer = new CsvWriter(
                                     new OutputStreamWriter(out,
                                             charsetName),
                                     csvConfig)) {
                            writer.writeValues(headerSet.stream()
                                    .collect(Collectors.toList()));
                            writer.flush();
                            Files.copy(outputFile.toPath(), out);
                            finished = true;
                        } finally {
                            if (finished) {
                                Files.copy(tempFile, outputFile.toPath(),
                                        StandardCopyOption.REPLACE_EXISTING);
                            }
                            Files.delete(tempFile);
                        }
                    }
                    listener.onResponse(null);
                } else {
                    client.prepareSearchScroll(scrollId)
                            .setScroll(RequestUtil.getScroll(request))
                            .execute(this);
                }
            } catch (final Exception e) {
                onFailure(e);
            }
        }

        @Override
        public void onFailure(final Exception e) {
            try {
                close();
            } catch (final Exception e1) {
                // ignore
            }
            listener.onFailure(new ElasticsearchException("Failed to write data.",
                    e));
        }

        private void close() {
            if (csvWriter != null) {
                try {
                    csvWriter.close();
                } catch (final IOException e) {
                    throw new ElasticsearchException("Could not close "
                            + outputFile.getAbsolutePath(), e);
                }
            }
        }
    }
}
