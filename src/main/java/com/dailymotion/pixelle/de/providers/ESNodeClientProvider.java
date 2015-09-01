package com.dailymotion.pixelle.de.providers;

import com.dailymotion.pixelle.de.processor.DeException;
import com.dailymotion.pixelle.de.processor.DeHelper;
import com.google.inject.Provider;
import com.google.inject.ProvisionException;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicPropertyFactory;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.dailymotion.pixelle.de.processor.DeHelper.adunitsType;
import static com.dailymotion.pixelle.de.processor.DeHelper.channelIndex;
import static com.dailymotion.pixelle.de.processor.DeHelper.clusterName;
import static com.dailymotion.pixelle.de.processor.DeHelper.dataDirectory;
import static com.dailymotion.pixelle.de.processor.DeHelper.nodeName;
import static com.dailymotion.pixelle.de.processor.DeHelper.organicIndex;
import static com.dailymotion.pixelle.de.processor.DeHelper.promotedIndex;
import static com.dailymotion.pixelle.de.processor.DeHelper.videosType;
import static com.dailymotion.pixelle.de.providers.ESIndexTypeFactory.createIndex;
import static com.netflix.config.DynamicPropertyFactory.getInstance;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 10/29/14.
 */
public class ESNodeClientProvider implements Provider<Client> {
    private static final DynamicBooleanProperty resetOrganic =
            getInstance().getBooleanProperty("index.organic.reset", false);
    private static final DynamicBooleanProperty resetPromoted =
            getInstance().getBooleanProperty("index.promoted.reset", false);
    private static final DynamicBooleanProperty resetChannel =
            getInstance().getBooleanProperty("index.channel.reset", false);

    private static final Logger LOGGER = getLogger(ESNodeClientProvider.class);

    /**
     * Provider that creates the indices, mapping and then returns the client for use in the application.
     *
     * @return es client
     */
    public Client get() {
        Builder elasticsearchSettings = settingsBuilder()
                .put("node.name", nodeName.get())
                .put("path.data", dataDirectory.get());

        Builder promotedSettings = settingsBuilder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.refresh_interval", "1s");

        Builder organicSettings = settingsBuilder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 0)
                .put("index.refresh_interval", "30s");

        Builder channelSettings = settingsBuilder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 0)
                .put("index.refresh_interval", "30s");


        Client client = nodeBuilder()
                .settings(elasticsearchSettings)
                .clusterName(clusterName.get())
                .client(false)
                .local(true)
                .node()
                .client();

        if (resetOrganic.get()
                && client.admin().indices().prepareExists(organicIndex.get()).execute().actionGet().isExists()
                && client.admin().indices().prepareDelete(organicIndex.get()).execute().actionGet().isAcknowledged()) {
            LOGGER.info("successfully deleted index: " + organicIndex.get());
        }
        if (resetPromoted.get()
                && client.admin().indices().prepareExists(promotedIndex.get()).execute().actionGet().isExists()
                && client.admin().indices().prepareDelete(promotedIndex.get()).execute().actionGet().isAcknowledged()) {
            LOGGER.info("successfully deleted index: " + promotedIndex.get());
        }
        if (resetChannel.get()
                && client.admin().indices().prepareExists(channelIndex.get()).execute().actionGet().isExists()
                && client.admin().indices().prepareDelete(channelIndex.get()).execute().actionGet().isAcknowledged()) {
            LOGGER.info("successfully deleted index: " + channelIndex.get());
        }
        //creates index only if it does not exist
        try {
            createIndex(client, promotedIndex.get(), promotedSettings.build(), adunitsType.get());
            createIndex(client, organicIndex.get(), organicSettings.build(), videosType.get());
            createIndex(client, channelIndex.get(), channelSettings.build(), videosType.get());

        } catch (DeException e) {
            LOGGER.error(e.getMessage());
            throw new ProvisionException(e.getMessage());
        }
        return client;
    }
}
