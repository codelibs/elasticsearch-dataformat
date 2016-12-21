package org.codelibs.elasticsearch.df.util;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.codelibs.elasticsearch.df.netty.Netty3HttpProvider;
import org.codelibs.elasticsearch.df.netty.Netty4HttpProvider;
import org.codelibs.elasticsearch.df.netty.NettyHttpProvider;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.rest.RestChannel;

public final class NettyUtils {
    private static final String DELEGATE_FIELD_NAME = "delegate";

    private static final String PIPELINEDREQUEST_FIELD_NAME = "pipelinedRequest";

    private static boolean initialized = false;

    private static boolean netty3 = false;

    private NettyUtils() {
    }

    public static boolean isNetty3(final RestChannel channel) {
        if (!initialized) {
            updateFields(channel);
        }
        return netty3;
    }

    public static boolean isNetty4(final RestChannel channel) {
        return !isNetty3(channel);
    }

    public static NettyHttpProvider getHttpProvider(final RestChannel channel) {
        try {
            if (isNetty3(channel)) {
                return new Netty3HttpProvider(channel);
            } else {
                return new Netty4HttpProvider(channel);
            }
        } catch (IllegalAccessException e) {
            throw new ElasticsearchException("Could not get http provider.");
        }
    }

    public synchronized static void updateFields(final RestChannel channel) {
        final Field delegateField = AccessController
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

        try {
            AccessController
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
                        } catch (final IllegalAccessException | NoSuchFieldException e) {
                            throw new ElasticsearchException(e);
                        }
                    }
                });
            netty3 = false;
        } catch (ElasticsearchException e) {
            netty3 = true;
        }
        initialized = true;
    }
}
