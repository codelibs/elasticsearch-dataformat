package org.codelibs.elasticsearch.df.util;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.rest.RestChannel;
import org.jboss.netty.channel.Channel;

public final class NettyUtils {
    private static final String CHANNEL_FIELD_NAME = "channel";

    private static final String DELEGATE_FIELD_NAME = "delegate";

    private static final ESLogger logger = Loggers.getLogger(NettyUtils.class);

    private static Field channelField;

    private static Field delegateField;

    private NettyUtils() {
    }

    public static Channel getChannel(final RestChannel channel) {
        try {
            if (channelField == null && delegateField == null) {
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
                } else if (channelField == null) {
                    channelField = AccessController
                            .doPrivileged(new PrivilegedAction<Field>() {
                                @Override
                                public Field run() {
                                    try {
                                        final Field field = channel.getClass()
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
            if (delegateField != null) {
                return (Channel) channelField.get(delegateField.get(channel));
            } else if (channelField != null) {
                return (Channel) channelField.get(channel);
            }
        } catch (final Exception e) {
            logger.error("Could not load Netty's channel.", e);
        }
        return null;
    }
}
