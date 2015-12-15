package com.dailymotion.pixelle.de.providers;

import com.dailymotion.pixelle.de.processor.DeException;
import com.google.inject.Provider;
import com.google.inject.ProvisionException;
import com.netflix.config.DynamicBooleanProperty;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;

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

    private static final Logger logger = getLogger(ESNodeClientProvider.class);

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

        // https://www.elastic.co/guide/en/elasticsearch/guide/current/shingles.html#_searching_for_shingles?q=shingle
        /**
         * Not only are shingles more flexible than phrase queries, but they perform better as well.
         * Instead of paying the price of a phrase query every time you search, queries for shingles are just as
         * efficient as a simple match query. A small price is paid at index time, because more terms need to be indexed,
         * which also means that fields with shingles use more disk space. However, most applications write once and read many times,
         * so it makes sense to optimize for fast queries.
         *
         * We use shingles for keyword search in title and description fields
         */
        Builder organicSettings = settingsBuilder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.refresh_interval", "30s")
                .put("analysis.filter.my_shingles_filter.type", "shingle")
                .put("analysis.filter.my_shingles_filter.min_shingle_size", 2)
                .put("analysis.filter.my_shingles_filter.max_shingle_size", 2)
                        // The shingle token filter outputs unigrams by default, but we want to keep unigrams and bigrams
                        // separate.
                .put("analysis.filter.my_shingles_filter.output_unigrams", false)
                .put("analysis.analyzer.my_shingles_analyzer.type", "custom")
                .put("analysis.analyzer.my_shingles_analyzer.tokenizer", "standard")
                .put("analysis.analyzer.my_shingles_analyzer.filter", "lowercase,my_shingles_filter");


        Builder channelSettings = settingsBuilder()
                .put("index.number_of_shards", 1)
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
            logger.info("successfully deleted index: " + organicIndex.get());
        }
        if (resetPromoted.get()
                && client.admin().indices().prepareExists(promotedIndex.get()).execute().actionGet().isExists()
                && client.admin().indices().prepareDelete(promotedIndex.get()).execute().actionGet().isAcknowledged()) {
            logger.info("successfully deleted index: " + promotedIndex.get());
        }
        if (resetChannel.get()
                && client.admin().indices().prepareExists(channelIndex.get()).execute().actionGet().isExists()
                && client.admin().indices().prepareDelete(channelIndex.get()).execute().actionGet().isAcknowledged()) {
            logger.info("successfully deleted index: " + channelIndex.get());
        }
        //creates index only if it does not exist
        try {
            createIndex(client, promotedIndex.get(), promotedSettings.build(), adunitsType.get());
            createIndex(client, organicIndex.get(), organicSettings.build(), videosType.get());
            createIndex(client, channelIndex.get(), channelSettings.build(), videosType.get());

        } catch (DeException e) {
            logger.error(e.getMessage());
            throw new ProvisionException(e.getMessage());
        }
        return client;
    }
}
