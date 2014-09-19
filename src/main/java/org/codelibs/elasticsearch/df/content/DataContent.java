package org.codelibs.elasticsearch.df.content;

import java.io.File;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.netty.buffer.ChannelBuffer;
import org.elasticsearch.rest.RestRequest;

public abstract class DataContent {

    protected RestRequest request;

    protected ChannelBuffer channelBuffer;
    
    protected Client client;

    public DataContent(final Client client, final RestRequest request) {
        this.client = client;
        this.request = request;
    }

    public abstract void write(File outputFile, SearchResponse response,
            ActionListener<Void> listener);

}