package org.codelibs.elasticsearch.df.content;

import java.io.File;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;

public abstract class DataContent {

    protected RestRequest request;

    protected Client client;

    protected ContentType contentType;

    public DataContent(final Client client, final RestRequest request, final ContentType contentType) {
        this.client = client;
        this.request = request;
        this.contentType = contentType;
    }

    public abstract void write(File outputFile, SearchResponse response, RestChannel channel,
            ActionListener<Void> listener);

    public RestRequest getRequest() {
        return request;
    }

    public ContentType getContentType() {
        return contentType;
    }
}