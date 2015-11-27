package org.codelibs.elasticsearch.df;

import org.codelibs.elasticsearch.df.rest.RestDataAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestModule;

public class DataFormatPlugin extends Plugin {
    @Override
    public String name() {
        return "DataFormatPlugin";
    }

    @Override
    public String description() {
        return "This plugin provides several response formats.";
    }

    // for Rest API
    public void onModule(final RestModule module) {
        module.addRestAction(RestDataAction.class);
    }

}
