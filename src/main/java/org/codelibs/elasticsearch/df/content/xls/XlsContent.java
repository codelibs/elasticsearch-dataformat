package org.codelibs.elasticsearch.df.content.xls;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.codelibs.elasticsearch.df.content.ContentType;
import org.codelibs.elasticsearch.df.content.DataContent;
import org.codelibs.elasticsearch.df.util.MapUtils;
import org.codelibs.elasticsearch.df.util.RequestUtil;
import org.codelibs.elasticsearch.df.util.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

public class XlsContent extends DataContent {
    private static final Logger logger = Loggers.getLogger(XlsContent.class);

    private static final int SXSSF_FLUSH_COUNT = 1000;

    private static final String DEFAULT_HEADER_COLUMN = "-";

    private final boolean appendHeader;

    private Set<String> headerSet;

    private boolean modifiableFieldSet;

    private final boolean isExcel2007;

    public XlsContent(final Client client, final RestRequest request, final ContentType contentType, final boolean isExcel2007) {
        super(client, request, contentType);

        appendHeader = request.paramAsBoolean("append.header", true);
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

        this.isExcel2007 = isExcel2007;

        if (logger.isDebugEnabled()) {
            logger.debug("appendHeader: " + appendHeader + ", headerSet: "
                    + headerSet + ", isExcel2007: " + isExcel2007);
        }
    }

    @Override
    public void write(final File outputFile, final SearchResponse response, final RestChannel channel,
                      final ActionListener<Void> listener) {
        try {
            final OnLoadListener onLoadListener = new OnLoadListener(outputFile, listener);
            onLoadListener.onResponse(response);
        } catch (final Exception e) {
            listener.onFailure(new ElasticsearchException("Failed to write data.", e));
        }
    }

    protected class OnLoadListener implements ActionListener<SearchResponse> {

        protected ActionListener<Void> listener;

        protected File outputFile;

        private Workbook workbook;

        private Sheet sheet;

        private int currentRowNumber = 0;

        protected OnLoadListener(final File outputFile, final ActionListener<Void> listener) {
            this.outputFile = outputFile;
            this.listener = listener;

            if (isExcel2007) {
                final SecurityManager sm = System.getSecurityManager();
                if (sm != null) {
                    sm.checkPermission(new SpecialPermission());
                }
                workbook = AccessController
                        .doPrivileged((PrivilegedAction<Workbook>) () -> new SXSSFWorkbook(-1));
                sheet = AccessController
                        .doPrivileged((PrivilegedAction<Sheet>) () -> workbook.createSheet());
            } else {
                workbook = new HSSFWorkbook();
                sheet = workbook.createSheet();
            }
        }

        private void flushSheet(final int currentCount, final Sheet sheet)
                throws IOException {
            if (sheet instanceof SXSSFSheet) {
                if (currentCount % SXSSF_FLUSH_COUNT == 0) {
                    ((SXSSFSheet) sheet).flushRows(0);
                }
            }
        }

        private void disposeWorkbook(final Workbook workbook) {
            if (workbook instanceof SXSSFWorkbook) {
                ((SXSSFWorkbook) workbook).dispose();
            }
        }

        @Override
        public void onResponse(final SearchResponse response) {
            final String scrollId = response.getScrollId();

            try {
                final SearchHits hits = response.getHits();

                final int size = hits.getHits().length;
                if (logger.isDebugEnabled()) {
                    logger.debug("scrollId: " + scrollId + ", totalHits: "
                            + hits.getTotalHits() + ", hits: " + size
                            + ", current: " + (currentRowNumber + size));
                }
                for (final SearchHit hit : hits) {
                    final Map<String, Object> sourceMap = hit.getSourceAsMap();
                    final Map<String, Object> dataMap = new HashMap<>();
                    MapUtils.convertToFlatMap("", sourceMap, dataMap);

                    for (final String key : dataMap.keySet()) {
                        if (modifiableFieldSet && !headerSet.contains(key)) {
                            headerSet.add(key);
                        }
                    }

                    final Row row = sheet.createRow(appendHeader ? currentRowNumber + 1
                                    : currentRowNumber);

                    int count = 0;
                    for (final String name : headerSet) {
                        final Object value = dataMap.get(name);
                        final Cell cell = row.createCell(count);
                        if (value != null
                                && value.toString().trim().length() > 0) {
                            cell.setCellValue(value.toString());
                        } else {
                            cell.setCellValue(DEFAULT_HEADER_COLUMN);
                        }
                        count++;
                    }

                    flushSheet(currentRowNumber, sheet);
                    currentRowNumber++;
                }

                if (size == 0 || scrollId == null) {
                    if (appendHeader) {
                        final Row headerRow = sheet.createRow(0);
                        int count = 0;
                        for (final String value : headerSet) {
                            final Cell cell = headerRow.createCell(count);
                            cell.setCellValue(value);
                            count++;
                        }
                    }
                    flushSheet(0, sheet);
                    try (OutputStream stream = new BufferedOutputStream(new FileOutputStream(outputFile))) {
                        final SecurityManager sm = System.getSecurityManager();
                        if (sm != null) {
                            sm.checkPermission(new SpecialPermission());
                        }
                        AccessController
                                .doPrivileged((PrivilegedAction<Void>) () -> {
                                    try {
                                        workbook.write(stream);
                                    } catch (final IOException e) {
                                        throw new ElasticsearchException(e);
                                    }
                                    return null;
                                });

                        stream.flush();
                    } finally {
                        disposeWorkbook(workbook);
                    }
                    // end
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
            listener.onFailure(new ElasticsearchException("Failed to write data.",
                    e));
        }

    }
}
