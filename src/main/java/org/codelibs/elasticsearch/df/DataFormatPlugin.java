package org.codelibs.elasticsearch.df;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import org.codelibs.elasticsearch.df.rest.RestDataAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

public class DataFormatPlugin extends Plugin implements ActionPlugin {
    @Override
    public List<RestHandler> getRestHandlers(final Settings settings,
            final RestController restController, final ClusterSettings clusterSettings,
            final IndexScopedSettings indexScopedSettings,
            final SettingsFilter settingsFilter,
            final IndexNameExpressionResolver indexNameExpressionResolver,
            final Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(new RestDataAction(settings, restController));
    }
}
