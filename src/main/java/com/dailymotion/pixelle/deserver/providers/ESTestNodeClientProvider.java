package com.dailymotion.pixelle.deserver.providers;

import com.dailymotion.pixelle.deserver.processor.DeHelper;
import com.google.inject.Provider;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Created by n.dhupia on 10/29/14.
 */
public final class ESTestNodeClientProvider implements Provider<Client> {
    private static final Logger logger = LoggerFactory.getLogger(ESTestNodeClientProvider.class);

    protected ESTestNodeClientProvider() {
    }

    /**
     * Provider for testing. This creates the indices, mapping and then returns the client.
     *
     * @return es client
     */
    public Client get() {
        ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder()
                .put("node.name", DeHelper.nodeName.get())
                .put("index.store.type", "memory")
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.refresh_interval", "1ms");


        Client client = nodeBuilder()
                .settings(elasticsearchSettings)
                .clusterName(DeHelper.clusterName.get())
                .data(true)
                .client(false)
                .local(true)
                .node()
                .client();

        if (client.admin().indices().prepareExists(DeHelper.organicIndex.get()).execute().actionGet().isExists()
                && client.admin().indices().prepareDelete(DeHelper.organicIndex.get()).execute().actionGet().isAcknowledged()) {
            logger.info("successfully deleted index: " + DeHelper.organicIndex.get());
        }
        if (client.admin().indices().prepareExists(DeHelper.promotedIndex.get()).execute().actionGet().isExists()
                && client.admin().indices().prepareDelete(DeHelper.promotedIndex.get()).execute().actionGet().isAcknowledged()) {
            logger.info("successfully deleted index: " + DeHelper.promotedIndex.get());
        }
        if (client.admin().indices().prepareExists(DeHelper.channelIndex.get()).execute().actionGet().isExists()
                && client.admin().indices().prepareDelete(DeHelper.channelIndex.get()).execute().actionGet().isAcknowledged()) {
            logger.info("successfully deleted index: " + DeHelper.channelIndex.get());
        }


        ESIndexTypeFactory.createIndex(client, DeHelper.promotedIndex.get(), elasticsearchSettings.build(), DeHelper.adunitsType.get());
        ESIndexTypeFactory.createIndex(client, DeHelper.organicIndex.get(), elasticsearchSettings.build(), DeHelper.videosType.get());
        ESIndexTypeFactory.createIndex(client, DeHelper.channelIndex.get(), elasticsearchSettings.build(), DeHelper.videosType.get());
        return client;
    }
}
