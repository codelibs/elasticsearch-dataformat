package org.codelibs.elasticsearch.df.util;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.rest.RestChannel;
import org.jboss.netty.channel.Channel;

public final class NettyUtils {
    private static final String CHANNEL_FIELD_NAME = "channel";

    private static final ESLogger logger = Loggers.getLogger(NettyUtils.class);

    private static Field channelField;

    private NettyUtils() {
    }

    public static Channel getChannel(final RestChannel channel) {
        try {
            if (channelField == null) {
                channelField = channel.getClass().getDeclaredField(
                        CHANNEL_FIELD_NAME);
                final SecurityManager sm = System.getSecurityManager();
                if (sm != null) {
                    sm.checkPermission(new SpecialPermission());
                }
                AccessController.doPrivileged(new PrivilegedAction<Void>() {
                    @Override
                    public Void run() {
                        channelField.setAccessible(true);
                        return null;
                    }
                });
            }
            return (Channel) channelField.get(channel);
        } catch (final Exception e) {
            logger.error("Could not load Netty's channel.", e);
        }
        return null;
    }
}
