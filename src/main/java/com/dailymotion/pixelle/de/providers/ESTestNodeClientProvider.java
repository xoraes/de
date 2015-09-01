package com.dailymotion.pixelle.de.providers;

import com.dailymotion.pixelle.de.processor.DeException;
import com.dailymotion.pixelle.de.processor.DeHelper;
import com.google.inject.Provider;
import com.google.inject.ProvisionException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.dailymotion.pixelle.de.processor.DeHelper.adunitsType;
import static com.dailymotion.pixelle.de.processor.DeHelper.channelIndex;
import static com.dailymotion.pixelle.de.processor.DeHelper.clusterName;
import static com.dailymotion.pixelle.de.processor.DeHelper.nodeName;
import static com.dailymotion.pixelle.de.processor.DeHelper.organicIndex;
import static com.dailymotion.pixelle.de.processor.DeHelper.promotedIndex;
import static com.dailymotion.pixelle.de.processor.DeHelper.videosType;
import static com.dailymotion.pixelle.de.providers.ESIndexTypeFactory.createIndex;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 10/29/14.
 */
public final class ESTestNodeClientProvider implements Provider<Client> {
    private static final Logger LOGGER = getLogger(ESTestNodeClientProvider.class);

    protected ESTestNodeClientProvider() {
    }

    /**
     * Provider for testing. This creates the indices, mapping and then returns the client.
     *
     * @return es client
     */
    public Client get() {
        Builder elasticsearchSettings = settingsBuilder()
                .put("node.name", nodeName.get())
                .put("index.store.type", "memory")
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.refresh_interval", "1s");


        Client client = nodeBuilder()
                .settings(elasticsearchSettings)
                .clusterName(clusterName.get())
                .data(true)
                .client(false)
                .local(true)
                .node()
                .client();

        if (client.admin().indices().prepareExists(organicIndex.get()).execute().actionGet().isExists()
                && client.admin().indices().prepareDelete(organicIndex.get()).execute().actionGet().isAcknowledged()) {
            LOGGER.info("successfully deleted index: " + organicIndex.get());
        }
        if (client.admin().indices().prepareExists(promotedIndex.get()).execute().actionGet().isExists()
                && client.admin().indices().prepareDelete(promotedIndex.get()).execute().actionGet().isAcknowledged()) {
            LOGGER.info("successfully deleted index: " + promotedIndex.get());
        }
        if (client.admin().indices().prepareExists(channelIndex.get()).execute().actionGet().isExists()
                && client.admin().indices().prepareDelete(channelIndex.get()).execute().actionGet().isAcknowledged()) {
            LOGGER.info("successfully deleted index: " + channelIndex.get());
        }


        try {
            createIndex(client, promotedIndex.get(), elasticsearchSettings.build(), adunitsType.get());
            createIndex(client, organicIndex.get(), elasticsearchSettings.build(), videosType.get());
            createIndex(client, channelIndex.get(), elasticsearchSettings.build(), videosType.get());
        } catch (DeException e) {
            LOGGER.error(e.getMessage());
            throw new ProvisionException(e.getMessage());
        }
        return client;
    }
}
