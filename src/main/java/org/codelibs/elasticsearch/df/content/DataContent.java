package org.codelibs.elasticsearch.df.content;

import java.io.File;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.netty.buffer.ChannelBuffer;
import org.elasticsearch.rest.RestRequest;

public abstract class DataContent {

    protected RestRequest request;

    protected ChannelBuffer channelBuffer;

    protected Client client;

    protected SearchType searchType;

    private boolean firstScan = true;

    public DataContent(final Client client, final RestRequest request,
            final SearchType searchType) {
        this.client = client;
        this.request = request;
        this.searchType = searchType;
    }

    protected boolean isFirstScan() {
        if (searchType == SearchType.SCAN && firstScan) {
            firstScan = false;
            return true;
        }
        return false;
    }

    public abstract void write(File outputFile, SearchResponse response,
            ActionListener<Void> listener);

}