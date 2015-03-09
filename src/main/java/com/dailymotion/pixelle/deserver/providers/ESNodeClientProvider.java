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
    private static DynamicBooleanProperty resetChannel =
            DynamicPropertyFactory.getInstance().getBooleanProperty("index.channel.reset", false);

    private static Logger logger = LoggerFactory.getLogger(ESNodeClientProvider.class);

    /**
     * Provider that creates the indices, mapping and then returns the client for use in the application.
     *
     * @return es client
     */
    public Client get() {
        ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder()
                .put("node.name", DeHelper.nodeName.get())
                .put("path.data", DeHelper.dataDirectory.get());

        ImmutableSettings.Builder promotedSettings = ImmutableSettings.settingsBuilder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.refresh_interval", "1s");

        ImmutableSettings.Builder organicSettings = ImmutableSettings.settingsBuilder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 0)
                .put("index.refresh_interval", "30s");

        ImmutableSettings.Builder channelSettings = ImmutableSettings.settingsBuilder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 0)
                .put("index.refresh_interval", "30s");


        Client client = nodeBuilder()
                .settings(elasticsearchSettings)
                .clusterName(DeHelper.clusterName.get())
                .client(false)
                .local(true)
                .node()
                .client();

        if (resetOrganic.get()
                && client.admin().indices().prepareExists(DeHelper.organicIndex.get()).execute().actionGet().isExists()
                && client.admin().indices().prepareDelete(DeHelper.organicIndex.get()).execute().actionGet().isAcknowledged()) {
            logger.info("successfully deleted index: " + DeHelper.organicIndex.get());
        }
        if (resetPromoted.get()
                && client.admin().indices().prepareExists(DeHelper.promotedIndex.get()).execute().actionGet().isExists()
                && client.admin().indices().prepareDelete(DeHelper.promotedIndex.get()).execute().actionGet().isAcknowledged()) {
            logger.info("successfully deleted index: " + DeHelper.promotedIndex.get());
        }
        if (resetChannel.get()
                && client.admin().indices().prepareExists(DeHelper.channelIndex.get()).execute().actionGet().isExists()
                && client.admin().indices().prepareDelete(DeHelper.channelIndex.get()).execute().actionGet().isAcknowledged()) {
            logger.info("successfully deleted index: " + DeHelper.channelIndex.get());
        }
        //creates index only if it does not exist
        ESIndexTypeFactory.createIndex(client, DeHelper.promotedIndex.get(), promotedSettings.build(), DeHelper.adunitsType.get());
        ESIndexTypeFactory.createIndex(client, DeHelper.organicIndex.get(), organicSettings.build(), DeHelper.videosType.get());
        ESIndexTypeFactory.createIndex(client, DeHelper.channelIndex.get(), channelSettings.build(), DeHelper.videosType.get());
        return client;
    }
}
