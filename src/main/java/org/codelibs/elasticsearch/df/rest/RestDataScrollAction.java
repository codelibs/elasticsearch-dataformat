package org.codelibs.elasticsearch.df.rest;

import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.codelibs.elasticsearch.df.DfContentException;
import org.codelibs.elasticsearch.df.content.ContentType;
import org.codelibs.elasticsearch.df.content.DataContent;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchOperationThreading;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IgnoreIndices;
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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.http.netty.NettyHttpChannel;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.seasar.util.beans.BeanDesc;
import org.seasar.util.beans.FieldDesc;
import org.seasar.util.beans.factory.BeanDescFactory;
import org.seasar.util.io.CloseableUtil;
import org.seasar.util.lang.FieldUtil;

public class RestDataScrollAction extends BaseRestHandler {

    @Inject
    public RestDataScrollAction(final Settings settings, final Client client,
            final RestController restController) {
        super(settings, client);

        restController.registerHandler(GET, "/_data/scroll", this);
        restController.registerHandler(POST, "/_data/scroll", this);
        restController.registerHandler(GET, "/{index}/_data/scroll", this);
        restController.registerHandler(POST, "/{index}/_data/scroll", this);
        restController.registerHandler(GET, "/{index}/{type}/_data/scroll",
                this);
        restController.registerHandler(POST, "/{index}/{type}/_data/scroll",
                this);
    }

    @Override
    public void handleRequest(final RestRequest request,
            final RestChannel channel) {
        final TimeValue keepAlive;
        final String scroll = request.param("scroll");
        if (scroll != null) {
            keepAlive = parseTimeValue(scroll, null);
        } else {
            keepAlive = new TimeValue(60000);
        }

        SearchRequestBuilder prepareSearch;
        try {
            final String[] indices = Strings.splitStringByCommaToArray(request
                    .param("index"));
            prepareSearch = client.prepareSearch(indices);
            // get the content, and put it in the body
            if (request.hasContent()) {
                prepareSearch.setSource(request.content(),
                        request.contentUnsafe());
            } else {
                final String source = request.param("source");
                if (source != null) {
                    prepareSearch.setSource(source);
                }
            }
            // add extra source based on the request parameters
            final XContentBuilder builder = XContentFactory
                    .contentBuilder(XContentType.JSON);
            final SearchSourceBuilder extraSource = RestSearchAction
                    .parseSearchSource(request);
            if (extraSource != null) {
                prepareSearch.setExtraSource(extraSource.toXContent(builder,
                        ToXContent.EMPTY_PARAMS));
            }

            prepareSearch.setSearchType(request.param("search_type"));

            prepareSearch.setScroll(keepAlive);

            prepareSearch.setTypes(Strings.splitStringByCommaToArray(request
                    .param("type")));
            prepareSearch.setRouting(request.param("routing"));
            prepareSearch.setPreference(request.param("preference"));
            if (request.hasParam("ignore_indices")) {
                prepareSearch.setIgnoreIndices(IgnoreIndices.fromString(request
                        .param("ignore_indices")));
            }

            prepareSearch.setListenerThreaded(false);
            SearchOperationThreading operationThreading = SearchOperationThreading
                    .fromString(request.param("operation_threading"), null);
            if (operationThreading != null) {
                if (operationThreading == SearchOperationThreading.NO_THREADS) {
                    // since we don't spawn, don't allow no_threads, but change
                    // it to a single thread
                    operationThreading = SearchOperationThreading.SINGLE_THREAD;
                }
                prepareSearch.setOperationThreading(operationThreading);
            }
        } catch (final Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("failed to parse search request parameters", e);
            }
            try {
                final XContentBuilder builder = restContentBuilder(request);
                channel.sendResponse(new XContentRestResponse(request,
                        BAD_REQUEST, builder.startObject()
                                .field("error", e.getMessage()).endObject()));
            } catch (final IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }

        prepareSearch.execute(new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(final SearchResponse response) {

                final ContentType contentType = getContentType(request);
                if (contentType == null) {
                    try {
                        final XContentBuilder builder = restContentBuilder(request);
                        channel.sendResponse(new XContentRestResponse(
                                request,
                                BAD_REQUEST,
                                builder.startObject()
                                        .field("error",
                                                "Unknown content type: "
                                                        + request
                                                                .header(HttpHeaders.Names.CONTENT_TYPE))
                                        .endObject()));
                    } catch (final IOException e) {
                        logger.error("Failed to send failure response", e);
                    }
                    return;
                }

                File outputFile = null;
                try {
                    outputFile = File.createTempFile("es_df_output_", ".dat");
                    final DataContent dataContent = contentType.dataContent(
                            client, request, response, keepAlive);
                    dataContent.write(outputFile);

                    writeResponse(request, channel, contentType, outputFile);
                } catch (final IOException e) {
                    onFailure(e);
                } finally {
                    if (outputFile != null && !outputFile.delete()) {
                        logger.warn("Failed to delete: "
                                + outputFile.getAbsolutePath());
                    }
                }
            }

            @Override
            public void onFailure(final Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(
                            request, e));
                } catch (final IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }

        });
    }

    private ContentType getContentType(final RestRequest request) {
        final String contentType = request.param("format",
                request.header("Content-Type"));
        if ("text/csv".equals(contentType)
                || "text/comma-separated-values".equals(contentType)
                || "csv".equalsIgnoreCase(contentType)) {
            return ContentType.CSV;
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

                final BeanDesc channelDesc = BeanDescFactory
                        .getBeanDesc(NettyHttpChannel.class);
                final FieldDesc channelField = channelDesc
                        .getFieldDesc("channel");
                final Channel nettyChannel = FieldUtil.get(
                        channelField.getField(), channel);
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
                CloseableUtil.close(fis);
            }
        } else {
            throw new DfContentException("The channel is not NettyHttpChannel.");
        }
    }
}
