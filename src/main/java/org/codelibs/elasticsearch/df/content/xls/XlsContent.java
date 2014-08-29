package org.codelibs.elasticsearch.df.content.xls;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.codelibs.elasticsearch.df.DfContentException;
import org.codelibs.elasticsearch.df.content.DataContent;
import org.codelibs.elasticsearch.df.util.MapUtil;
import org.codelibs.elasticsearch.df.util.RequestUtil;
import org.codelibs.elasticsearch.util.lang.StringUtils;
import org.codelibs.elasticsearch.util.netty.NettyUtils;
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

    private static final String DEFAULT_HEADER_COLUMN = "-";

    private boolean appnedHeader;

    private Set<String> headerSet;

    private boolean modifiableFieldSet;

    private final Channel nettyChannel;

    public XlsContent(final Client client, final RestRequest request,
            final RestChannel channel) {
        super(client, request);

        appnedHeader = request.paramAsBoolean("append.header", true);
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
            logger.debug("appnedHeader: " + appnedHeader + ", headerSet: "
                    + headerSet + ", nettyChannel: " + nettyChannel);
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

        protected File outputFile;

        private int currentCount = 0;

        protected OnLoadListener(final File outputFile,
                final ActionListener<Void> listener) {
            this.outputFile = outputFile;
            this.listener = listener;
        }

        @Override
        public void onResponse(final SearchResponse response) {
            if (!isConnected()) {
                onFailure(new DfContentException("Disconnected."));
                return;
            }

            Workbook workbook;
            Sheet sheet;
            try {
                if (outputFile.exists()) {
                    InputStream stream = null;
                    try {
                        stream = new BufferedInputStream(new FileInputStream(
                                outputFile));
                        workbook = new HSSFWorkbook(stream);
                    } finally {
                        if (stream != null) {
                            try {
                                stream.close();
                            } catch (final IOException e) {
                                // ignore
                            }
                        }
                    }
                    sheet = workbook.getSheetAt(0);
                } else {
                    workbook = new HSSFWorkbook();
                    sheet = workbook.createSheet();
                }

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
                        final Row headerRow = sheet.createRow(currentCount);
                        int count = 0;
                        for (final String value : headerSet) {
                            final Cell cell = headerRow.createCell(count);
                            cell.setCellValue(value);
                            count++;
                        }
                        appnedHeader = false;
                    }

                    currentCount++;
                    final Row row = sheet
                            .createRow(appnedHeader ? currentCount + 1
                                    : currentCount);

                    int count = 0;
                    for (final String name : headerSet) {
                        Object value = dataMap.get(name);
                        final Cell cell = row.createCell(count);
                        if (value != null
                                && value.toString().trim().length() > 0) {
                            cell.setCellValue(value.toString());
                        } else {
                            cell.setCellValue(DEFAULT_HEADER_COLUMN);
                        }
                        count++;
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
                        } catch (final IOException e) {
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
            listener.onFailure(new DfContentException("Failed to write data.",
                    e));
        }

    }
}
