package com.dailymotion.pixelle.deserver.providers;

import com.dailymotion.pixelle.deserver.processor.DeHelper;
import com.google.inject.Provider;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicPropertyFactory;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Created by n.dhupia on 10/29/14.
 */
public class ESNodeClientProvider implements Provider<Client> {
    private static DynamicBooleanProperty resetOrganic =
            DynamicPropertyFactory.getInstance().getBooleanProperty("index.organic.reset", false);
    private static DynamicBooleanProperty resetPromoted =
            DynamicPropertyFactory.getInstance().getBooleanProperty("index.promoted.reset", false);

    private static Logger logger = LoggerFactory.getLogger(ESNodeClientProvider.class);

    /**
     * Provider that creates the indices, mapping and then returns the client for use in the application.
     *
     * @return es client
     */
    public Client get() {
        ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder()
                .put("node.name", DeHelper.getNode())
                .put("path.data", DeHelper.getDataDir())
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 0)
                .put("index.refresh_interval", "30s");

        Client client = nodeBuilder()
                .settings(elasticsearchSettings)
                .clusterName(DeHelper.getCluster())
                .client(false)
                .local(true)
                .node()
                .client();

        if (resetOrganic.get()
                && client.admin().indices().prepareExists(DeHelper.getOrganicIndex()).execute().actionGet().isExists()
                && client.admin().indices().prepareDelete(DeHelper.getOrganicIndex()).execute().actionGet().isAcknowledged()) {
            logger.info("successfully deleted index: " + DeHelper.getOrganicIndex());
        }
        if (resetPromoted.get()
                && client.admin().indices().prepareExists(DeHelper.getPromotedIndex()).execute().actionGet().isExists()
                && client.admin().indices().prepareDelete(DeHelper.getPromotedIndex()).execute().actionGet().isAcknowledged()) {
            logger.info("successfully deleted index: " + DeHelper.getPromotedIndex());
        }
        //creates index only if it does not exist
        ESIndexTypeFactory.createIndex(client, DeHelper.getPromotedIndex(), elasticsearchSettings.build(), DeHelper.getAdUnitsType());
        ESIndexTypeFactory.createIndex(client, DeHelper.getOrganicIndex(), elasticsearchSettings.build(), DeHelper.getVideosType());
        return client;
    }
}
