package org.codelibs.elasticsearch.df.content.xls;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import jp.sf.orangesignal.csv.CsvConfig;
import jp.sf.orangesignal.csv.CsvWriter;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
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
 
public class XlsContent extends DataContent {
    private static final ESLogger logger = Loggers.getLogger(XlsContent.class);
    private final String charsetName;
    private final CsvConfig csvConfig;
    private boolean appnedHeader;
    private Set<String> headerSet;
    private boolean modifiableFieldSet;
    private final Channel nettyChannel;

    public XlsContent(final Client client, final RestRequest request,
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

        private int currentCount = 0;

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

            Workbook workbook;
            try {
                if (outputFile.isFile()) {
                    InputStream stream = null;
                    try {
                        stream = new BufferedInputStream(new FileInputStream(
                                outputFile));
                        workbook = new HSSFWorkbook(stream);
                    } finally {
                        if (stream != null) {
                            try {
                                stream.close();
                            } catch (IOException e) {
                                // ignore
                            }
                        }
                    }
                } else {
                    workbook = new HSSFWorkbook();
                }
                Sheet sheet = workbook.createSheet();

                final SearchHits hits = response.getHits();

                final int size = hits.getHits().length;
                logger.info("scrollId: " + response.getScrollId()
                        + ", totalHits: " + hits.totalHits() + ", hits: "
                        + size + ", current: " + (currentCount + size));

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
                        Row headerRow = sheet.createRow(1);
                        int count = 0;
                        for (String value : headerSet) {
                            Cell cell = headerRow.createCell(count);
                            cell.setCellValue(value);
                            count++;
                        }
                        appnedHeader = false;
                    }

                    currentCount++;
                    Row row = sheet.createRow(appnedHeader ? currentCount + 1
                            : currentCount);

                    int count = 0;
                    for (final String name : headerSet) {
                        final Object value = dataMap.get(name);
                        if (value != null) {
                            Cell cell = row.createCell(count);
                            cell.setCellValue(value.toString());
                            count++;
                        }
                    }
                }

                OutputStream stream = null;
                try {
                    stream = new BufferedOutputStream(new FileOutputStream(
                            outputFile));
                    workbook.write(stream);
                    stream.flush();
                } finally {
                    if (stream != null) {
                        try {
                            stream.close();
                        } catch (IOException e) {
                            // ignore
                        }
                    }
                }
                if (size == 0) {
                    // end
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
