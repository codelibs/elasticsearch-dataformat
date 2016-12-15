package org.codelibs.elasticsearch.df;

import org.codelibs.elasticsearch.df.rest.RestDataAction;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;

import java.util.Arrays;
import java.util.List;

public class DataFormatPlugin extends Plugin implements ActionPlugin {
    @Override
    public List<Class<? extends RestHandler>> getRestHandlers() {
        return Arrays.asList(
            RestDataAction.class
        );
    }
}
