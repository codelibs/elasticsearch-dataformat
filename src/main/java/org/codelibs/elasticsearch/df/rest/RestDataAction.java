package org.codelibs.elasticsearch.df.rest;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.codelibs.elasticsearch.df.content.ContentType;
import org.codelibs.elasticsearch.df.content.DataContent;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.search.RestSearchAction;

public class RestDataAction extends BaseRestHandler {

    private static final float DEFAULT_LIMIT_PERCENTAGE = 10;

    private final long maxMemory;
    private final long defaultLimit;

    public RestDataAction(final Settings settings,
                          final RestController restController) {
        super(settings);

        restController.registerHandler(GET, "/_data", this);
        restController.registerHandler(POST, "/_data", this);
        restController.registerHandler(GET, "/{index}/_data", this);
        restController.registerHandler(POST, "/{index}/_data", this);
        restController.registerHandler(GET, "/{index}/{type}/_data", this);
        restController.registerHandler(POST, "/{index}/{type}/_data", this);

        this.maxMemory = Runtime.getRuntime().maxMemory();
        this.defaultLimit = (long) (maxMemory * (DEFAULT_LIMIT_PERCENTAGE / 100F));
    }

    public RestChannelConsumer prepareRequest(final RestRequest request,
            final NodeClient client) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        request.withContentOrSourceParamParserOrNull(
                parser -> RestSearchAction.parseSearchRequest(searchRequest,
                        request, parser,
                        size -> searchRequest.source().size(size)));

        if (request.paramAsInt("size", -1) == -1) {
            searchRequest.source().size(100);
        }

        final String file = request.param("file");

        final long limitBytes;
        String limitParamStr = request.param("limit");
        if (Strings.isNullOrEmpty(limitParamStr)) {
            limitBytes = defaultLimit;
        } else {
            if (limitParamStr.endsWith("%")) {
                limitParamStr = limitParamStr.substring(0,
                        limitParamStr.length() - 1);
            }
            limitBytes = (long) (maxMemory
                    * (Float.parseFloat(limitParamStr) / 100F));
        }

        final ContentType contentType = getContentType(request);
        if (contentType == null) {
            final String msg = "Unknown content type:"
                    + request.header("Content-Type");
            throw new IllegalArgumentException(msg);
        }
        final DataContent dataContent = contentType.dataContent(client,
                request);

        return channel -> client.search(searchRequest, new SearchResponseListener(
                channel, file, limitBytes, dataContent));
    }

    /**
     * Retrieve dump format (csv, excel, or json) from {@link RestRequest}
     *
     * @param request
     * @return a {@link ContentType} value
     */
    private ContentType getContentType(final RestRequest request) {
        final String contentType = request.param("format",
                request.header("Content-Type"));
        if (logger.isDebugEnabled()) {
            logger.debug("contentType: " + contentType);
        }
        if ("text/csv".equals(contentType)
                || "text/comma-separated-values".equals(contentType)
                || "csv".equalsIgnoreCase(contentType)) {
            return ContentType.CSV;
        } else if ("application/excel".equals(contentType)
                || "application/msexcel".equals(contentType)
                || "application/vnd.ms-excel".equals(contentType)
                || "application/x-excel".equals(contentType)
                || "application/x-msexcel".equals(contentType)
                || "xls".equalsIgnoreCase(contentType)) {
            return ContentType.EXCEL;
        } else if ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                .equals(contentType) || "xlsx".equalsIgnoreCase(contentType)) {
            return ContentType.EXCEL2007;
        } else if ("text/javascript".equals(contentType)
                || "application/json".equals(contentType)
                || "json".equalsIgnoreCase(contentType)) {
            return ContentType.JSON;
        }

        return null;
    }

    class SearchResponseListener implements ActionListener<SearchResponse> {

        private final RestChannel channel;

        private File outputFile;

        private final DataContent dataContent;

        private final long limit;

        SearchResponseListener(final RestChannel channel, final String file, final long limit,
                               final DataContent dataContent) {
            this.channel = channel;
            this.dataContent = dataContent;
            if (!Strings.isNullOrEmpty(file)) {
                outputFile = new File(file);
                final File parentFile = outputFile.getParentFile();
                if (parentFile != null && !parentFile.isDirectory()) {
                    throw new ElasticsearchException("Cannot create/access "
                            + outputFile.getAbsolutePath());
                }
            }
            this.limit = limit;
        }

        @Override
        public void onResponse(final SearchResponse response) {

            try {
                final boolean useLocalFile = outputFile != null;
                if (outputFile == null) {
                    outputFile = File.createTempFile("es_df_output_", ".dat");
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("outputFile: " + outputFile.getAbsolutePath());
                }
                dataContent.write(outputFile, response, channel,
                        new ActionListener<Void>() {

                            @Override
                            public void onResponse(final Void response) {
                                try {
                                    if (useLocalFile) {
                                        // from java 8: the local variables passed to anonymous class
                                        // could also be "effectively final", which means their values
                                        // are never changed after initialization.
                                        // it's more about to encourage the use of lambda expression
                                        // instead of creating anonymous class
                                        sendResponse(dataContent.getRequest(), channel,
                                                outputFile.getAbsolutePath());
                                    } else {
                                        writeResponse(dataContent.getRequest(), channel, outputFile, limit, dataContent);
                                        SearchResponseListener.this
                                                .deleteOutputFile();
                                    }
                                } catch (final Exception e) {
                                    onFailure(e);
                                }
                            }

                            @Override
                            public void onFailure(final Exception e) {
                                SearchResponseListener.this.onFailure(e);
                            }
                        });
            } catch (final IOException e) {
                onFailure(e);
            }
        }

        private void deleteOutputFile() {
            if (outputFile != null && !outputFile.delete()) {
                logger.warn("Failed to delete: " + outputFile.getAbsolutePath());
            }
        }

        @Override
        public void onFailure(final Exception e) {
            deleteOutputFile();
            try {
                channel.sendResponse(new BytesRestResponse(channel,
                        RestStatus.INTERNAL_SERVER_ERROR, e));
            } catch (final IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
        }

        private void sendResponse(final RestRequest request, final RestChannel channel, final String file) {
            try {
                final XContentBuilder builder = JsonXContent.contentBuilder();
                final String pretty = request.param("pretty");
                if (pretty != null && !"false".equalsIgnoreCase(pretty)) {
                    builder.prettyPrint().lfAtEnd();
                }
                builder.startObject();
                builder.field("acknowledged", true);
                builder.field("file", file);
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(OK, builder));
            } catch (final IOException e) {
                throw new ElasticsearchException("Failed to create a resposne.", e);
            }
        }

        private void writeResponse(final RestRequest request, final RestChannel channel, final File outputFile,
                                   final long limit, final DataContent dataContent) {
            if (outputFile.length() > limit) {
                onFailure(new ElasticsearchException("Content size is too large " + outputFile.length()));
                return;
            }

            try (FileInputStream fis = new FileInputStream(outputFile)) {
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                final byte[] bytes = new byte[1024];
                int len;
                while ((len = fis.read(bytes)) > 0) {
                    out.write(bytes, 0, len);
                }

                final ContentType contentType = dataContent.getContentType();
                final BytesRestResponse response = new BytesRestResponse(
                        RestStatus.OK, contentType.contentType(),
                        out.toByteArray());
                response.addHeader("Content-Disposition",
                        "attachment; filename=\""
                                + contentType.fileName(request) + "\"");
                channel.sendResponse(response);
            } catch (final Throwable e) {
                throw new ElasticsearchException("Failed to render the content.", e);
            }
        }
    }

    @Override
    public String getName() {
        return "data_download_action";
    }
}
