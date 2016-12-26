package org.codelibs.elasticsearch.df.netty;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.rest.RestChannel;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;

public class Netty3HttpProvider implements NettyHttpProvider {
    private static final String CHANNEL_FIELD_NAME = "channel";

    private static final String DELEGATE_FIELD_NAME = "delegate";

    private static Field channelField;

    private static Field delegateField;

    private final Channel nettyChannel;

    public Netty3HttpProvider(final RestChannel channel) throws IllegalAccessException {
        if (delegateField == null) {
            updateFields(channel);
        }

        this.nettyChannel = (Channel) channelField.get(delegateField.get(channel));
    }

    @Override
    public void writeResponse(Map<String, Object> headers, FileInputStream fis) throws IOException {
        try(final FileChannel fileChannel = fis.getChannel()) {
            final DefaultHttpResponse nettyResponse = new DefaultHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            for (final String key : headers.keySet()) {
                nettyResponse.headers().set(key, headers.get(key));
            }


            final MappedByteBuffer buffer = fileChannel.map(
                FileChannel.MapMode.READ_ONLY, 0L, fileChannel.size());
            final ChannelBuffer channelBuffer = ChannelBuffers
                .wrappedBuffer(buffer);
            nettyResponse.setContent(channelBuffer);

            nettyChannel.write(nettyResponse);
        }
    }

    @Override
    public boolean isOpenConnection() {
        return nettyChannel.isConnected();
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
        }
    }
}
