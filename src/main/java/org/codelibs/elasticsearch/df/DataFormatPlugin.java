package org.codelibs.elasticsearch.df;

import org.codelibs.elasticsearch.df.rest.RestDataAction;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

public class DataFormatPlugin extends AbstractPlugin {
    @Override
    public String name() {
        return "DataFormatPlugin";
    }

    @Override
    public String description() {
        return "This is a elasticsearch-dataformat plugin.";
    }

    // for Rest API
    public void onModule(final RestModule module) {
        module.addRestAction(RestDataAction.class);
    }

}
