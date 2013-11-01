package org.codelibs.elasticsearch.df.content;

import java.io.File;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestRequest;

public abstract class DataContent {

    protected RestRequest request;

    protected Client client;

    protected SearchResponse response;

    protected TimeValue keepAlive;

    public DataContent(final Client client, final RestRequest request,
            final SearchResponse response, final TimeValue keepAlive) {
        this.client = client;
        this.request = request;
        this.response = response;
        this.keepAlive = keepAlive;
    }

    public abstract void write(File outputFile);

}