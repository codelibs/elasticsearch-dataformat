package org.codelibs.elasticsearch.df.content.json;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.logging.log4j.Logger;
import org.codelibs.elasticsearch.df.content.ContentType;
import org.codelibs.elasticsearch.df.content.DataContent;
import org.codelibs.elasticsearch.df.util.RequestUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

public class JsonContent extends DataContent {
    private static final Logger logger = Loggers.getLogger(JsonContent.class);

    private final String bulkIndex;

    private final String bulkType;

    public JsonContent(final Client client, final RestRequest request, final ContentType contentType) {
        super(client, request, contentType);

        bulkIndex = request.param("bulk.index");
        bulkType = request.param("bulk.type");
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
        }

        @Override
        public void onResponse(final SearchResponse response) {
            final String scrollId = response.getScrollId();
            final SearchHits hits = response.getHits();
            final int size = hits.getHits().length;
            currentCount += size;
            if (logger.isDebugEnabled()) {
                logger.debug("scrollId: " + scrollId + ", totalHits: "
                        + hits.getTotalHits() + ", hits: " + size + ", current: "
                        + currentCount);
            }
            try {
                for (final SearchHit hit : hits) {
                    final String index = bulkIndex == null ? hit.getIndex()
                            : bulkIndex;
                    final String type = bulkType == null ? hit.getType()
                            : bulkType;
                    final String operation = "{\"index\":{\"_index\":\""
                            + index + "\",\"_type\":\"" + type
                            + "\",\"_id\":\"" + hit.getId() + "\"}}";
                    final String source = XContentHelper.convertToJson(
                            hit.getSourceRef(), true, false, XContentType.JSON);
                    writer.append(operation).append('\n');
                    writer.append(source).append('\n');
                }

                if (size == 0 || scrollId == null) {
                    // end
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
