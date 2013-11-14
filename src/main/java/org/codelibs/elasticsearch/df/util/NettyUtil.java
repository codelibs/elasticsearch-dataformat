package org.codelibs.elasticsearch.df.util;

import org.elasticsearch.common.netty.channel.Channel;
import org.elasticsearch.http.netty.NettyHttpChannel;
import org.elasticsearch.rest.RestChannel;
import org.seasar.util.beans.BeanDesc;
import org.seasar.util.beans.FieldDesc;
import org.seasar.util.beans.factory.BeanDescFactory;
import org.seasar.util.lang.FieldUtil;

public class NettyUtil {
    protected NettyUtil() {
    }

    public static Channel getChannel(final RestChannel channel) {
        final BeanDesc channelDesc = BeanDescFactory
                .getBeanDesc(NettyHttpChannel.class);
        final FieldDesc channelField = channelDesc.getFieldDesc("channel");
        final Channel nettyChannel = FieldUtil.get(channelField.getField(),
                channel);
        return nettyChannel;
    }
}
