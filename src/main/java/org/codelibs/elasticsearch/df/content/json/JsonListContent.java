package org.codelibs.elasticsearch.df.content.json;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.elasticsearch.df.content.ContentType;
import org.codelibs.elasticsearch.df.content.DataContent;
import org.codelibs.elasticsearch.df.util.RequestUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

public class JsonListContent extends DataContent {
    private static final Logger logger = LogManager.getLogger(JsonListContent.class);

    public JsonListContent(final Client client, final RestRequest request, final ContentType contentType) {
        super(client, request, contentType);
    }

    @Override
    public void write(final File outputFile, final SearchResponse response, final RestChannel channel,
            final ActionListener<Void> listener) {
        try {
            final OnLoadListener onLoadListener = new OnLoadListener(
                    outputFile, listener);
            onLoadListener.onResponse(response);
        } catch (final Exception e) {
            listener.onFailure(new ElasticsearchException("Failed to write data.",
                    e));
        }
    }

    protected class OnLoadListener implements ActionListener<SearchResponse> {
        protected ActionListener<Void> listener;

        protected Writer writer;

        protected File outputFile;

        private long currentCount = 0;

        private boolean firstLine = true;

        protected OnLoadListener(final File outputFile, final ActionListener<Void> listener) {
            this.outputFile = outputFile;
            this.listener = listener;
            try {
                writer = new BufferedWriter(new OutputStreamWriter(
                        new FileOutputStream(outputFile), "UTF-8"));
            } catch (final Exception e) {
                throw new ElasticsearchException("Could not open "
                        + outputFile.getAbsolutePath(), e);
            }
            try {
                writer.append('[');
            }catch (final Exception e) {
                onFailure(e);
            }
        }

        @Override
        public void onResponse(final SearchResponse response) {
            final String scrollId = response.getScrollId();
            final SearchHits hits = response.getHits();
            final int size = hits.getHits().length;
            currentCount += size;
            if (logger.isDebugEnabled()) {
                logger.debug("scrollId: {}, totalHits: {}, hits: {}, current: {}",
                        scrollId, hits.getTotalHits(), size, currentCount);
            }
            try {
                for (final SearchHit hit : hits) {
                    final String source = XContentHelper.convertToJson(
                            hit.getSourceRef(), true, false, XContentType.JSON);
                    if (!firstLine){
                        writer.append(',');
                    }else{
                        firstLine = false;
                    }
                    writer.append('\n').append(source);
                }

                if (size == 0 || scrollId == null) {
                    // end
                    writer.append('\n').append(']');
                    writer.flush();
                    close();
                    listener.onResponse(null);
                } else {
                    client.prepareSearchScroll(scrollId)
                            .setScroll(RequestUtil.getScroll(request))
                            .execute(this);
                }
            } catch (final Exception e) {
                onFailure(e);
            }
        }

        @Override
        public void onFailure(final Exception e) {
            try {
                close();
            } catch (final Exception e1) {
                // ignore
            }
            listener.onFailure(new ElasticsearchException("Failed to write data.",
                    e));
        }

        private void close() {
            if (writer != null) {
                try {
                    writer.close();
                } catch (final IOException e) {
                    throw new ElasticsearchException("Could not close "
                            + outputFile.getAbsolutePath(), e);
                }
            }
        }
    }
}
