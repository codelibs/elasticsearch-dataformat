package org.codelibs.elasticsearch.df.content.json;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.codelibs.elasticsearch.df.DfContentException;
import org.codelibs.elasticsearch.df.content.DataContent;
import org.codelibs.elasticsearch.df.util.RequestUtil;
import org.codelibs.elasticsearch.util.netty.NettyUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.netty.channel.Channel;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

public class JsonContent extends DataContent {
    private static final ESLogger logger = Loggers.getLogger(JsonContent.class);

    private final Channel nettyChannel;

    public JsonContent(final Client client, final RestRequest request,
            final RestChannel channel) {
        super(client, request);

        nettyChannel = NettyUtils.getChannel(channel);

    }

    @Override
    public void write(final File outputFile, final SearchResponse response,
            final ActionListener<Void> listener) {
        try {
            final OnLoadListener onLoadListener = new OnLoadListener(
                    outputFile, listener);
            onLoadListener.onResponse(response);
        } catch (final Exception e) {
            listener.onFailure(new DfContentException("Failed to write data.",
                    e));
        }
    }

    protected class OnLoadListener implements ActionListener<SearchResponse> {
        protected ActionListener<Void> listener;

        protected Writer writer;

        protected File outputFile;

        private long currentCount = 0;

        protected OnLoadListener(final File outputFile,
                final ActionListener<Void> listener) {
            this.outputFile = outputFile;
            this.listener = listener;
            try {
                writer = new BufferedWriter(new OutputStreamWriter(
                        new FileOutputStream(outputFile), "UTF-8"));
            } catch (final Exception e) {
                throw new DfContentException("Could not open "
                        + outputFile.getAbsolutePath(), e);
            }
        }

        @Override
        public void onResponse(final SearchResponse response) {
            if (!isConnected()) {
                onFailure(new DfContentException("Disconnected."));
                return;
            }

            final SearchHits hits = response.getHits();

            final int size = hits.getHits().length;
            currentCount += size;
            logger.info("scrollId: " + response.getScrollId() + ", totalHits: "
                    + hits.totalHits() + ", hits: " + size + ", current: "
                    + currentCount);

            try {
                for (final SearchHit hit : hits) {
                    String operation = "{\"index\":{\"_index\":\""
                            + hit.index() + "\",\"_type\":\"" + hit.type()
                            + "\",\"_id\":\"" + hit.id() + "\"}}";
                    String source = hit.sourceAsString();
                    writer.append(operation).append('\n');
                    writer.append(source).append('\n');
                }

                if (size == 0) {
                    // end
                    writer.flush();
                    close();
                    listener.onResponse(null);
                } else {
                    client.prepareSearchScroll(response.getScrollId())
                            .setScroll(RequestUtil.getScroll(request))
                            .execute(this);
                }
            } catch (final Exception e) {
                onFailure(e);
            }
        }

        private boolean isConnected() {
            return nettyChannel != null && nettyChannel.isConnected();
        }

        @Override
        public void onFailure(final Throwable e) {
            try {
                close();
            } catch (final Exception e1) {
                // ignore
            }
            listener.onFailure(new DfContentException("Failed to write data.",
                    e));
        }

        private void close() {
            if (writer != null) {
                try {
                    writer.close();
                } catch (final IOException e) {
                    throw new DfContentException("Could not close "
                            + outputFile.getAbsolutePath(), e);
                }
            }
        }
    }
}
