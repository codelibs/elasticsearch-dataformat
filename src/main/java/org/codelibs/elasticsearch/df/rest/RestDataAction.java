package org.codelibs.elasticsearch.df.rest;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.search.suggest.SuggestBuilder.termSuggestion;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.codelibs.elasticsearch.df.DfContentException;
import org.codelibs.elasticsearch.df.content.ContentType;
import org.codelibs.elasticsearch.df.content.DataContent;
import org.codelibs.elasticsearch.df.util.RequestUtil;
import org.codelibs.elasticsearch.util.IOUtils;
import org.codelibs.elasticsearch.util.NettyUtils;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.netty.buffer.ChannelBuffer;
import org.elasticsearch.common.netty.buffer.ChannelBuffers;
import org.elasticsearch.common.netty.channel.Channel;
import org.elasticsearch.common.netty.handler.codec.http.DefaultHttpResponse;
import org.elasticsearch.common.netty.handler.codec.http.HttpHeaders;
import org.elasticsearch.common.netty.handler.codec.http.HttpResponseStatus;
import org.elasticsearch.common.netty.handler.codec.http.HttpVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.http.netty.NettyHttpChannel;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

public class RestDataAction extends BaseRestHandler {

    private static final String[] emptyStrings = new String[0];

    @Inject
    public RestDataAction(final Settings settings, final Client client,
            final RestController restController) {
        super(settings, client);

        restController.registerHandler(GET, "/_data", this);
        restController.registerHandler(POST, "/_data", this);
        restController.registerHandler(GET, "/{index}/_data", this);
        restController.registerHandler(POST, "/{index}/_data", this);
        restController.registerHandler(GET, "/{index}/{type}/_data", this);
        restController.registerHandler(POST, "/{index}/{type}/_data", this);
    }

    @Override
    public void handleRequest(final RestRequest request,
            final RestChannel channel) {

        SearchRequestBuilder prepareSearch;
        try {
            final String[] indices = request.paramAsStringArray("index",
                    emptyStrings);
            if (logger.isDebugEnabled()) {
                logger.debug("indices: " + indices);
            }
            prepareSearch = client.prepareSearch(indices);
            // get the content, and put it in the body
            if (request.hasContent()) {
                prepareSearch.setSource(request.content(),
                        request.contentUnsafe());
            } else {
                final String source = request.param("source");
                if (source != null) {
                    prepareSearch.setSource(source);
                    if (logger.isDebugEnabled()) {
                        logger.debug("source: " + source);
                    }
                }
            }
            // add extra source based on the request parameters
            final XContentBuilder builder = XContentFactory
                    .contentBuilder(XContentType.JSON);
            final SearchSourceBuilder extraSource = parseSearchSource(request);
            if (extraSource != null) {
                prepareSearch.setExtraSource(extraSource.toXContent(builder,
                        ToXContent.EMPTY_PARAMS));
            }

            prepareSearch.setSearchType(request.param("search_type"));

            prepareSearch.setScroll(RequestUtil.getScroll(request));

            final String[] types = request.paramAsStringArray("type",
                    emptyStrings);
            if (types.length > 0) {
                prepareSearch.setTypes(types);
            }
            prepareSearch.setRouting(request.param("routing"));
            prepareSearch.setPreference(request.param("preference"));
            prepareSearch.setIndicesOptions(IndicesOptions.fromRequest(request, IndicesOptions.strict()));

            prepareSearch.setListenerThreaded(false);
            prepareSearch.execute(new SearchResponseListener(request, channel));
        } catch (final Exception e) {
            logger.error("failed to parse search request parameters", e);
            try {
            	final XContentBuilder builder = channel.newBuilder();
            	builder.startObject()
            			.field("error", e.getMessage())
            			.endObject();
            	channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, builder));
            	  
            } catch (final IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
        }
    }

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
        } else if ("text/javascript".equals(contentType)
                || "application/json".equals(contentType)
                || "json".equalsIgnoreCase(contentType)) {
            return ContentType.JSON;
        }

        return null;
    }

    private void writeResponse(final RestRequest request,
            final RestChannel channel, final ContentType contentType,
            final File outputFile) {
        if (channel instanceof NettyHttpChannel) {
            final DefaultHttpResponse nettyResponse = new DefaultHttpResponse(
                    HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            nettyResponse.setHeader(HttpHeaders.Names.CONTENT_TYPE,
                    contentType.contentType());
            nettyResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH,
                    outputFile.length());
            nettyResponse.setHeader("Content-Disposition",
                    "attachment; filename=\"" + contentType.fileName(request)
                            + "\"");
            nettyResponse.setHeader(HttpHeaders.Names.COOKIE, "fileDownload=true; path=/");

            FileInputStream fis = null;
            FileChannel fileChannel = null;
            try {
                fis = new FileInputStream(outputFile);
                fileChannel = fis.getChannel();

                final MappedByteBuffer buffer = fileChannel.map(
                        FileChannel.MapMode.READ_ONLY, 0L, fileChannel.size());
                final ChannelBuffer channelBuffer = ChannelBuffers
                        .wrappedBuffer(buffer);
                nettyResponse.setContent(channelBuffer);

                final Channel nettyChannel = NettyUtils.getChannel(channel);
                nettyChannel.write(nettyResponse);
            } catch (final Exception e) {
                throw new DfContentException("Failed to render the content.", e);
            } finally {
                if (fileChannel != null) {
                    try {
                        fileChannel.close();
                    } catch (final IOException e) {
                        // ignore
                    }
                }
                IOUtils.closeQuietly(fis);
            }
        } else {
            throw new DfContentException("The channel is not NettyHttpChannel.");
        }
    }

    public static SearchSourceBuilder parseSearchSource(
            final RestRequest request) {
        SearchSourceBuilder searchSourceBuilder = null;
        final String queryString = request.param("q");
        if (queryString != null) {
            final QueryStringQueryBuilder queryBuilder = QueryBuilders
                    .queryString(queryString);
            queryBuilder.defaultField(request.param("df"));
            queryBuilder.analyzer(request.param("analyzer"));
            queryBuilder.analyzeWildcard(request.paramAsBoolean(
                    "analyze_wildcard", false));
            queryBuilder.lowercaseExpandedTerms(request.paramAsBoolean(
                    "lowercase_expanded_terms", true));
            queryBuilder.lenient(request
                    .paramAsBooleanOptional("lenient", null));
            final String defaultOperator = request.param("default_operator");
            if (defaultOperator != null) {
                if ("OR".equals(defaultOperator)) {
                    queryBuilder
                            .defaultOperator(QueryStringQueryBuilder.Operator.OR);
                } else if ("AND".equals(defaultOperator)) {
                    queryBuilder
                            .defaultOperator(QueryStringQueryBuilder.Operator.AND);
                } else {
                    throw new ElasticsearchIllegalArgumentException(
                            "Unsupported defaultOperator [" + defaultOperator
                                    + "], can either be [OR] or [AND]");
                }
            }
            if (searchSourceBuilder == null) {
                searchSourceBuilder = new SearchSourceBuilder();
            }
            searchSourceBuilder.query(queryBuilder);
        }

        final int from = request.paramAsInt("from", -1);
        if (from != -1) {
            if (searchSourceBuilder == null) {
                searchSourceBuilder = new SearchSourceBuilder();
            }
            searchSourceBuilder.from(from);
        }
        final int size = request.paramAsInt("size", -1);
        if (size != -1) {
            if (searchSourceBuilder == null) {
                searchSourceBuilder = new SearchSourceBuilder();
            }
            searchSourceBuilder.size(size);
        }

        if (request.hasParam("explain")) {
            if (searchSourceBuilder == null) {
                searchSourceBuilder = new SearchSourceBuilder();
            }
            searchSourceBuilder.explain(request.paramAsBooleanOptional(
                    "explain", null));
        }
        if (request.hasParam("version")) {
            if (searchSourceBuilder == null) {
                searchSourceBuilder = new SearchSourceBuilder();
            }
            searchSourceBuilder.version(request.paramAsBooleanOptional(
                    "version", null));
        }
        if (request.hasParam("timeout")) {
            if (searchSourceBuilder == null) {
                searchSourceBuilder = new SearchSourceBuilder();
            }
            searchSourceBuilder.timeout(request.paramAsTime("timeout", null));
        }

        final String sField = request.param("fields");
        if (sField != null) {
            if (searchSourceBuilder == null) {
                searchSourceBuilder = new SearchSourceBuilder();
            }
            if (!Strings.hasText(sField)) {
                searchSourceBuilder.noFields();
            } else {
                final String[] sFields = Strings
                        .splitStringByCommaToArray(sField);
                if (sFields != null) {
                    for (final String field : sFields) {
                        searchSourceBuilder.field(field);
                    }
                }
            }
        }

        final String sSorts = request.param("sort");
        if (sSorts != null) {
            if (searchSourceBuilder == null) {
                searchSourceBuilder = new SearchSourceBuilder();
            }
            final String[] sorts = Strings.splitStringByCommaToArray(sSorts);
            for (final String sort : sorts) {
                final int delimiter = sort.lastIndexOf(":");
                if (delimiter != -1) {
                    final String sortField = sort.substring(0, delimiter);
                    final String reverse = sort.substring(delimiter + 1);
                    if ("asc".equals(reverse)) {
                        searchSourceBuilder.sort(sortField, SortOrder.ASC);
                    } else if ("desc".equals(reverse)) {
                        searchSourceBuilder.sort(sortField, SortOrder.DESC);
                    }
                } else {
                    searchSourceBuilder.sort(sort);
                }
            }
        }

        final String sIndicesBoost = request.param("indices_boost");
        if (sIndicesBoost != null) {
            if (searchSourceBuilder == null) {
                searchSourceBuilder = new SearchSourceBuilder();
            }
            final String[] indicesBoost = Strings
                    .splitStringByCommaToArray(sIndicesBoost);
            for (final String indexBoost : indicesBoost) {
                final int divisor = indexBoost.indexOf(',');
                if (divisor == -1) {
                    throw new ElasticsearchIllegalArgumentException(
                            "Illegal index boost [" + indexBoost + "], no ','");
                }
                final String indexName = indexBoost.substring(0, divisor);
                final String sBoost = indexBoost.substring(divisor + 1);
                try {
                    searchSourceBuilder.indexBoost(indexName,
                            Float.parseFloat(sBoost));
                } catch (final NumberFormatException e) {
                    throw new ElasticsearchIllegalArgumentException(
                            "Illegal index boost [" + indexBoost
                                    + "], boost not a float number");
                }
            }
        }

        final String sStats = request.param("stats");
        if (sStats != null) {
            if (searchSourceBuilder == null) {
                searchSourceBuilder = new SearchSourceBuilder();
            }
            searchSourceBuilder
                    .stats(Strings.splitStringByCommaToArray(sStats));
        }

        final String suggestField = request.param("suggest_field");
        if (suggestField != null) {
            final String suggestText = request.param("suggest_text",
                    queryString);
            final int suggestSize = request.paramAsInt("suggest_size", 5);
            if (searchSourceBuilder == null) {
                searchSourceBuilder = new SearchSourceBuilder();
            }
            final String suggestMode = request.param("suggest_mode");
            searchSourceBuilder.suggest().addSuggestion(
                    termSuggestion(suggestField).field(suggestField)
                            .text(suggestText).size(suggestSize)
                            .suggestMode(suggestMode));
        }

        return searchSourceBuilder;
    }

    class SearchResponseListener implements ActionListener<SearchResponse> {
        private final RestRequest request;

        private final RestChannel channel;

        private File outputFile;

        SearchResponseListener(final RestRequest request,
                final RestChannel channel) {
            this.request = request;
            this.channel = channel;
        }

        @Override
        public void onResponse(final SearchResponse response) {

            final ContentType contentType = getContentType(request);
            if (contentType == null) {
                try {
                    final XContentBuilder builder = channel.newBuilder();
                    builder.startObject()
                    	.field("error",
                    			"Unknown content type:"
                    			+ request.header(HttpHeaders.Names.CONTENT_TYPE))
                    	.endObject();
                    channel.sendResponse(new BytesRestResponse(
                    		RestStatus.BAD_REQUEST,
                    		builder));
                
                } catch (final IOException e) {
                    logger.error("Failed to send failure response", e);
                }
                return;
            }

            try {
                outputFile = File.createTempFile("es_df_output_", ".dat");
                if (outputFile.delete()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("outputFile: "
                                + outputFile.getAbsolutePath());
                    }
                    final DataContent dataContent = contentType.dataContent(
                            client, request, channel);
                    dataContent.write(outputFile, response,
                            new ActionListener<Void>() {

                                @Override
                                public void onResponse(final Void response) {
                                    try {
                                        writeResponse(request, channel,
                                                contentType, outputFile);
                                        SearchResponseListener.this
                                                .deleteOutputFile();
                                    } catch (final Exception e) {
                                        onFailure(e);
                                    }
                                }

                                @Override
                                public void onFailure(final Throwable e) {
                                    SearchResponseListener.this.onFailure(e);
                                }
                            });
                } else {
                    onFailure(new DfContentException(
                            "Could not create a temporary file on the local system."));
                }
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
        public void onFailure(final Throwable e) {
            deleteOutputFile();
            try {
                channel.sendResponse(new BytesRestResponse(channel, RestStatus.INTERNAL_SERVER_ERROR, e));
            } catch (final IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
        }

    }
}
