package org.codelibs.elasticsearch.df.netty;

import io.netty.buffer.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.*;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.http.netty4.pipelining.HttpPipelinedRequest;
import org.elasticsearch.rest.RestChannel;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;

public class Netty4HttpProvider implements NettyHttpProvider {
    private static final String CHANNEL_FIELD_NAME = "channel";

    private static final String DELEGATE_FIELD_NAME = "delegate";

    private static final String NETTYREQUEST_FIELD_NAME = "nettyRequest";

    private static final String PIPELINEDREQUEST_FIELD_NAME = "pipelinedRequest";

    private static Field channelField;

    private static Field delegateField;

    private static Field pipelinedRequestField;

    private static Field nettyRequestField;

    private final Channel nettyChannel;

    private final HttpPipelinedRequest httpPipelinedRequest;

    private final FullHttpRequest nettyRequest;

    public Netty4HttpProvider(final RestChannel channel) throws IllegalAccessException {
        if (delegateField == null) {
            updateFields(channel);
        }

        this.nettyChannel = (Channel) channelField.get(delegateField.get(channel));
        this.httpPipelinedRequest = (HttpPipelinedRequest) pipelinedRequestField.get(delegateField.get(channel));
        this.nettyRequest = (FullHttpRequest) nettyRequestField.get(delegateField.get(channel));
    }

    @Override
    public void writeResponse(final Map<String, Object> headers, final FileInputStream fis) throws IOException {
        try(final FileChannel fileChannel = fis.getChannel()) {
            final ByteBuf byteBuf = Unpooled.wrappedBuffer(fileChannel.map(
                FileChannel.MapMode.READ_ONLY, 0L, fileChannel.size()));

            final FullHttpResponse nettyResponse = newResponse(byteBuf);
            for (final String key : headers.keySet()) {
                nettyResponse.headers().set(key, headers.get(key));
            }

            final ChannelPromise promise = nettyChannel.newPromise();

            if (isCloseConnection()) {
                promise.addListener(ChannelFutureListener.CLOSE);
            }
            if (httpPipelinedRequest == null) {
                nettyChannel.writeAndFlush(nettyResponse, promise);
            } else {
                nettyChannel.writeAndFlush(httpPipelinedRequest.createHttpResponse(nettyResponse, promise));
            }
        } finally {
            if (httpPipelinedRequest != null) {
                httpPipelinedRequest.release();
            }
        }
    }

    @Override
    public boolean isOpenConnection() {
        return nettyChannel.isOpen();
    }

    private boolean isHttp10() {
        return nettyRequest.protocolVersion().equals(HttpVersion.HTTP_1_0);
    }

    private boolean isCloseConnection() {
        final boolean http10 = isHttp10();
        return HttpHeaderValues.CLOSE.contentEqualsIgnoreCase(nettyRequest.headers().get(HttpHeaderNames.CONNECTION)) ||
            (http10 && !HttpHeaderValues.KEEP_ALIVE.contentEqualsIgnoreCase(nettyRequest.headers().get(HttpHeaderNames.CONNECTION)));
    }

    private FullHttpResponse newResponse(ByteBuf buffer) {
        final boolean http10 = isHttp10();
        final boolean close = isCloseConnection();
        final HttpResponseStatus status = HttpResponseStatus.OK;
        final FullHttpResponse response;
        if (http10) {
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_0, status, buffer);
            if (!close) {
                response.headers().add(HttpHeaderNames.CONNECTION, "Keep-Alive");
            }
        } else {
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, buffer);
        }
        return response;
    }

    private synchronized void updateFields(final RestChannel channel) {
        if (delegateField == null) {
            delegateField = AccessController
                .doPrivileged(new PrivilegedAction<Field>() {
                    @Override
                    public Field run() {
                        try {
                            final Field field = channel.getClass()
                                .getDeclaredField(
                                    DELEGATE_FIELD_NAME);
                            field.setAccessible(true);
                            return field;
                        } catch (final Exception e) {
                            throw new ElasticsearchException(e);
                        }
                    }
                });
            channelField = AccessController
                .doPrivileged(new PrivilegedAction<Field>() {
                    @Override
                    public Field run() {
                        try {
                            final Field field = delegateField
                                .get(channel).getClass()
                                .getDeclaredField(
                                    CHANNEL_FIELD_NAME);
                            field.setAccessible(true);
                            return field;
                        } catch (final Exception e) {
                            throw new ElasticsearchException(e);
                        }
                    }
                });
            pipelinedRequestField = AccessController
                .doPrivileged(new PrivilegedAction<Field>() {
                    @Override
                    public Field run() {
                        try {
                            final Field field = delegateField
                                .get(channel).getClass()
                                .getDeclaredField(
                                    PIPELINEDREQUEST_FIELD_NAME);
                            field.setAccessible(true);
                            return field;
                        } catch (final Exception e) {
                            throw new ElasticsearchException(e);
                        }
                    }
                });
            nettyRequestField = AccessController
                .doPrivileged(new PrivilegedAction<Field>() {
                    @Override
                    public Field run() {
                        try {
                            final Field field = delegateField
                                .get(channel).getClass()
                                .getDeclaredField(
                                    NETTYREQUEST_FIELD_NAME);
                            field.setAccessible(true);
                            return field;
                        } catch (final Exception e) {
                            throw new ElasticsearchException(e);
                        }
                    }
                });
        }
    }
}
