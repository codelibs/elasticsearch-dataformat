package org.codelibs.elasticsearch.df.util;

import java.lang.reflect.Field;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.netty.channel.Channel;
import org.elasticsearch.http.netty.NettyHttpChannel;
import org.elasticsearch.rest.RestChannel;

public class NettyUtils {
    private static final ESLogger logger = Loggers.getLogger(NettyUtils.class);

    private NettyUtils() {
    }

    private static Field channelField;

    static {
        try {
            Field channelField = NettyHttpChannel.class.getField("channel");
            channelField.setAccessible(true);
        } catch (Exception e) {
            logger.error("Could not load NettyHttpChannel.", e);
        }
    }

    public static Channel getChannel(final RestChannel channel) {
        if (channelField == null) {
            return null;
        }
        try {
            return (Channel) channelField.get(channel);
        } catch (Exception e) {
            logger.error("Could not load Netty's channel.", e);
        }
        return null;
 }
}
