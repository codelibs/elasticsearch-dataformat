package org.codelibs.elasticsearch.df.rest;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.search.suggest.SuggestBuilders.termSuggestion;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.codelibs.elasticsearch.df.content.ContentType;
import org.codelibs.elasticsearch.df.content.DataContent;
import org.codelibs.elasticsearch.df.util.RequestUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;

public class RestDataAction extends BaseRestHandler {

    private static final String[] emptyStrings = new String[0];

    private final SearchRequestParsers searchRequestParsers;

    private final long maxMemory;
    private final long defaultLimit;

    @Inject
    public RestDataAction(final Settings settings, final RestController restController, final SearchRequestParsers searchRequestParsers) {
        super(settings);

        this.searchRequestParsers = searchRequestParsers;

        restController.registerHandler(GET, "/_data", this);
        restController.registerHandler(POST, "/_data", this);
        restController.registerHandler(GET, "/{index}/_data", this);
        restController.registerHandler(POST, "/{index}/_data", this);
        restController.registerHandler(GET, "/{index}/{type}/_data", this);
        restController.registerHandler(POST, "/{index}/{type}/_data", this);

        this.maxMemory = Runtime.getRuntime().maxMemory();
        this.defaultLimit =  maxMemory / 10;
    }

    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        SearchRequestBuilder prepareSearch;
        try {
            final String[] indices = request.paramAsStringArray("index",
                emptyStrings);
            if (logger.isDebugEnabled()) {
                logger.debug("indices: " + Arrays.toString(indices));
            }
            prepareSearch = client.prepareSearch(indices);
            Object fromObj = request.param("from");
            // get the content, and put it in the body
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            BytesReference restContent = RestActions.hasBodyContent(request)?RestActions.getRestContent(request):null;
            if (restContent != null && restContent.length() > 0) {
                try (XContentParser parser = XContentFactory.xContent(restContent).createParser(restContent)) {
                    QueryParseContext context = new QueryParseContext(searchRequestParsers.queryParsers, parser, parseFieldMatcher);
                    searchSourceBuilder.parseXContent(context, searchRequestParsers.aggParsers, searchRequestParsers.suggesters, searchRequestParsers.searchExtParsers);
                }
                final Map<String, Object> map = SourceLookup
                    .sourceAsMap(restContent);
                fromObj = map.get("from");
            } else {
                final String source = request.param("source");
                if (source != null) {
                    try (XContentParser parser = XContentFactory.xContent(source).createParser(source)) {
                        QueryParseContext context = new QueryParseContext(searchRequestParsers.queryParsers, parser, parseFieldMatcher);
                        searchSourceBuilder.parseXContent(context, searchRequestParsers.aggParsers, searchRequestParsers.suggesters, searchRequestParsers.searchExtParsers);

                        if (logger.isDebugEnabled()) {
                            logger.debug("source: " + source);
                        }
                        final Map<String, Object> map = parser
                            .map();
                        fromObj = map.get("from");
                    }
                }
            }
            if (fromObj == null) {
                prepareSearch.setScroll(RequestUtil.getScroll(request));
            }
            // add search source based on the request parameters
            prepareSearch.setSource(searchSourceBuilder);
            parseSearchSource(searchSourceBuilder, request);

            if (request.hasParam("search_type")) {
                prepareSearch.setSearchType(request.param("search_type"));
            } else {
                prepareSearch.setSearchType("query_then_fetch");
            }

            final String[] types = request.paramAsStringArray("type",
                emptyStrings);
            if (types.length > 0) {
                prepareSearch.setTypes(types);
            }
            prepareSearch.setRouting(request.param("routing"));
            prepareSearch.setPreference(request.param("preference"));
            prepareSearch.setIndicesOptions(IndicesOptions.fromRequest(request,
                IndicesOptions.strictExpandOpen()));

            final String file = request.param("file");

            final ContentType contentType = getContentType(request);
            if (contentType == null) {
                final String msg = "Unknown content type:" + request.header("Content-Type");
                throw new IllegalArgumentException(msg);
            }

            final long limitBytes;
            String limitParamStr = request.param("limit");
            if (Strings.isNullOrEmpty(limitParamStr)) {
                limitBytes = defaultLimit;
            } else {
                if (limitParamStr.endsWith("%")) {
                    limitParamStr = limitParamStr.substring(0, limitParamStr.length() - 1);
                }
                limitBytes = (long)(maxMemory * (Float.parseFloat(limitParamStr) / 100F));
            }

            final DataContent dataContent = contentType.dataContent(client, request);
            return (channel) -> prepareSearch.execute(new SearchResponseListener(request, channel,
                client, prepareSearch.request().searchType(), file, limitBytes, dataContent));
        } catch (final Exception e) {
            logger.warn("failed to parse search request parameters", e);
            throw e;
        }
    }

    private static void parseSearchSource(final SearchSourceBuilder searchSourceBuilder, RestRequest request) {
        QueryBuilder queryBuilder = RestActions.urlParamsToQueryBuilder(request);
        if (queryBuilder != null) {
            searchSourceBuilder.query(queryBuilder);
        }

        int from = request.paramAsInt("from", -1);
        if (from != -1) {
            searchSourceBuilder.from(from);
        }
        int size = request.paramAsInt("size", -1);
        if (size != -1) {
            searchSourceBuilder.size(size);
        }

        if (request.hasParam("explain")) {
            searchSourceBuilder.explain(request.paramAsBoolean("explain", null));
        }
        if (request.hasParam("version")) {
            searchSourceBuilder.version(request.paramAsBoolean("version", null));
        }
        if (request.hasParam("timeout")) {
            searchSourceBuilder.timeout(request.paramAsTime("timeout", null));
        }
        if (request.hasParam("terminate_after")) {
            int terminateAfter = request.paramAsInt("terminate_after",
                SearchContext.DEFAULT_TERMINATE_AFTER);
            if (terminateAfter < 0) {
                throw new IllegalArgumentException("terminateAfter must be > 0");
            } else if (terminateAfter > 0) {
                searchSourceBuilder.terminateAfter(terminateAfter);
            }
        }

        if (request.param("fields") != null) {
            throw new IllegalArgumentException("The parameter [" +
                SearchSourceBuilder.FIELDS_FIELD + "] is no longer supported, please use [" +
                SearchSourceBuilder.STORED_FIELDS_FIELD + "] to retrieve stored fields or _source filtering " +
                "if the field is not stored");
        }


        StoredFieldsContext storedFieldsContext =
            StoredFieldsContext.fromRestRequest(SearchSourceBuilder.STORED_FIELDS_FIELD.getPreferredName(), request);
        if (storedFieldsContext != null) {
            searchSourceBuilder.storedFields(storedFieldsContext);
        }
        String sDocValueFields = request.param("docvalue_fields");
        if (sDocValueFields == null) {
            sDocValueFields = request.param("fielddata_fields");
        }
        if (sDocValueFields != null) {
            if (Strings.hasText(sDocValueFields)) {
                String[] sFields = Strings.splitStringByCommaToArray(sDocValueFields);
                for (String field : sFields) {
                    searchSourceBuilder.docValueField(field);
                }
            }
        }
        FetchSourceContext fetchSourceContext = FetchSourceContext.parseFromRestRequest(request);
        if (fetchSourceContext != null) {
            searchSourceBuilder.fetchSource(fetchSourceContext);
        }

        if (request.hasParam("track_scores")) {
            searchSourceBuilder.trackScores(request.paramAsBoolean("track_scores", false));
        }

        String sSorts = request.param("sort");
        if (sSorts != null) {
            String[] sorts = Strings.splitStringByCommaToArray(sSorts);
            for (String sort : sorts) {
                int delimiter = sort.lastIndexOf(":");
                if (delimiter != -1) {
                    String sortField = sort.substring(0, delimiter);
                    String reverse = sort.substring(delimiter + 1);
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

        String sStats = request.param("stats");
        if (sStats != null) {
            searchSourceBuilder.stats(Arrays.asList(Strings.splitStringByCommaToArray(sStats)));
        }

        String suggestField = request.param("suggest_field");
        if (suggestField != null) {
            String suggestText = request.param("suggest_text", request.param("q"));
            int suggestSize = request.paramAsInt("suggest_size", 5);
            String suggestMode = request.param("suggest_mode");
            searchSourceBuilder.suggest(new SuggestBuilder().addSuggestion(suggestField,
                termSuggestion(suggestField)
                    .text(suggestText).size(suggestSize)
                    .suggestMode(TermSuggestionBuilder.SuggestMode.resolve(suggestMode))));
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
        private final RestRequest request;

        private final RestChannel channel;

        private File outputFile;

        private Client client;

        private SearchType searchType;

        private DataContent dataContent;

        private long limit;

        SearchResponseListener(final RestRequest request,
                final RestChannel channel, final Client client, final SearchType searchType, final String file, final long limit, final DataContent dataContent) {
            this.request = request;
            this.channel = channel;
            this.client = client;
            this.searchType = searchType;
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
                                        sendResponse(request,channel,
                                                outputFile.getAbsolutePath());
                                    } else {
                                        writeResponse(request, channel, outputFile, limit, dataContent);
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

        private void sendResponse(final RestRequest request,final RestChannel channel, final String file) {
            try {
                final XContentBuilder builder = JsonXContent.contentBuilder();
                final String pretty=request.param("pretty");
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

            try (FileInputStream fis = new FileInputStream(outputFile)){
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                byte[] bytes = new byte[1024];
                int len;
                while((len = fis.read(bytes)) > 0) {
                    out.write(bytes, 0, len);
                }

                channel.sendResponse(new BytesRestResponse(RestStatus.OK, dataContent.getContentType().contentType(), out.toByteArray()));
            } catch (final Throwable e) {
                throw new ElasticsearchException("Failed to render the content.", e);
            }
        }
    }
}
