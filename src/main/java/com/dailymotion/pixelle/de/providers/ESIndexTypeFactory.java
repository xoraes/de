package com.dailymotion.pixelle.de.providers;

import com.dailymotion.pixelle.de.processor.DeException;
import com.google.inject.ProvisionException;
import com.netflix.config.DynamicIntProperty;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;

import java.io.IOException;

import static com.dailymotion.pixelle.de.processor.DeHelper.adunitsType;
import static com.dailymotion.pixelle.de.processor.DeHelper.organicIndex;
import static com.dailymotion.pixelle.de.processor.DeHelper.promotedIndex;
import static com.dailymotion.pixelle.de.processor.DeHelper.videosType;
import static com.netflix.config.DynamicPropertyFactory.getInstance;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 11/19/14.
 */
final class ESIndexTypeFactory {
    private static final Logger logger = getLogger(ESIndexTypeFactory.class);
    private static final DynamicIntProperty channelTtl =
            getInstance().getIntProperty("channel.index.ttl", 300000); // 5 minutes


    private ESIndexTypeFactory() {
    }

    /**
     * Creates an es index given name, settings and type. Type mapping is added automatically using typename.
     *
     * @param client
     * @param indexName
     * @param settings
     * @param typeName
     * @throws DeException
     */
    public static void createIndex(Client client, String indexName, Settings settings, String typeName) throws DeException {

        boolean ack;
        try {
            // create the index if it does not already exist. If it exists don't do anything.

            if (!client.admin().indices().prepareExists(indexName).execute().actionGet().isExists()) {
                ack = client.admin().indices()
                        .prepareCreate(indexName)
                        .setSettings(settings)
                        .execute()
                        .actionGet()
                        .isAcknowledged();
                if (ack) {
                    logger.info("Index creation succeeded");
                } else {
                    logger.info("Index already exists ! Not re-creating");
                }
            }

            if (!client.admin().indices()
                    .prepareTypesExists(indexName)
                    .setTypes(typeName)
                    .execute()
                    .actionGet()
                    .isExists()) {

                ack = client.admin()
                        .indices()
                        .preparePutMapping(indexName)
                        .setType(typeName)
                        .setSource(createMapping(indexName))
                        .execute()
                        .actionGet()
                        .isAcknowledged();

                if (ack) {
                    logger.info("type creation succeeded");
                }
            } else {
                logger.info("type adready exists");
            }

        } catch (IOException e) {
            logger.error(e.getMessage());
            //TODO Use throwableProvider instead of this
            throw new ProvisionException("Could not create index", e);
        }
    }

    private static XContentBuilder createMapping(String indexName) throws IOException {
        if (indexName.equalsIgnoreCase(promotedIndex.get())) {
            return createAdUnitMapping(adunitsType.get());
        } else if (indexName.equalsIgnoreCase(organicIndex.get())) {
            return createVideosMapping(videosType.get());
        } else {
            return createVideosMapping(videosType.get(), true);
        }
    }


    private static XContentBuilder createAdUnitMapping(String typeName) throws IOException {
        XContentBuilder builder = jsonBuilder().startObject().startObject(typeName).startObject("properties");
        builder.startObject("_created").field("type", "date").field("format", "date_time_no_millis").field("index", "not_analyzed").endObject();
        builder.startObject("_updated").field("type", "date").field("format", "date_time_no_millis").field("index", "not_analyzed").endObject();
        builder.startObject("start_date").field("type", "date").field("format", "date_time_no_millis").field("index", "not_analyzed").endObject();
        builder.startObject("end_date").field("type", "date").field("format", "date_time_no_millis").field("index", "not_analyzed").endObject();

        builder.startObject("_id").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("video_id").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("ad").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("campaign").field("type", "string").field("index", "not_analyzed").endObject();

        builder.startObject("locations").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("languages").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("categories").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("excluded_locations").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("excluded_categories").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("timetable").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("devices").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("status").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("formats").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("domain_blacklist").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("domain_whitelist").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("autoplay").field("type", "boolean").field("index", "not_analyzed").endObject();

        builder.startObject("cpc").field("type", "integer").endObject();
        builder.startObject("cpv").field("type", "integer").endObject();
        builder.startObject("internal_cpv").field("type", "integer").endObject();


        builder.startObject("clicks").field("type", "float").endObject();
        builder.startObject("views").field("type", "float").endObject();
        builder.startObject("impressions").field("type", "float").endObject();
        builder.startObject("goal_views").field("type", "float").endObject();

        builder.startObject("currency").field("type", "string").field("index", "no").endObject();
        builder.startObject("tactic").field("type", "string").field("index", "no").endObject();
        builder.startObject("title").field("type", "string").field("index", "no").endObject();
        builder.startObject("description").field("type", "string").field("index", "no").endObject();
        builder.startObject("thumbnail_url").field("type", "string").field("index", "no").endObject();
        builder.startObject("resizable_thumbnail_url").field("type", "string").field("index", "no").endObject();
        builder.startObject("video_url").field("type", "string").field("index", "no").endObject();

        builder.startObject("channel").field("type", "string").field("index", "no").endObject();
        builder.startObject("channel_id").field("type", "string").field("index", "no").endObject();
        builder.startObject("channel_name").field("type", "string").field("index", "no").endObject();
        builder.startObject("channel_url").field("type", "string").field("index", "no").endObject();
        builder.startObject("goal_period").field("type", "string").field("index", "no").endObject();
        builder.startObject("account").field("type", "string").field("index", "no").endObject();
        builder.startObject("delivery").field("type", "string").field("index", "no").endObject();
        builder.startObject("custom_video_url").field("type", "string").field("index", "no").endObject();

        builder.startObject("duration").field("type", "integer").field("index", "no").endObject();
        builder.startObject("paused").field("type", "boolean").field("index", "not_analyzed").endObject();

        builder.endObject().endObject().endObject();


        return builder;
    }

    private static XContentBuilder createVideosMapping(String typeName) throws IOException {
        return createVideosMapping(typeName, false);
    }

    private static XContentBuilder createVideosMapping(String typeName, Boolean isTtlSet) throws IOException {
        XContentBuilder builder = jsonBuilder().startObject().startObject(typeName).startObject("properties");
        builder.startObject("_created").field("type", "date").field("format", "date_time_no_millis").field("index", "not_analyzed").endObject();
        builder.startObject("_updated").field("type", "date").field("format", "date_time_no_millis").field("index", "not_analyzed").endObject();
        builder.startObject("publication_date").field("type", "date").field("format", "date_time_no_millis").field("index", "not_analyzed").endObject();

        builder.startObject("_id").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("video_id").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("languages").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("categories").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("tags").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("channel_tier").field("type", "string").field("index", "not_analyzed").endObject();

        //IMPORTANT: this causes titles and description to be indexed and analyzed. Field norm is disabled
        // because we should not prioritize fields with shorter length. Disabling norms can save a significant amount of memory.
        //Used the more modern BM25 similar algorithm than the default IF/TDF
        //https://www.elastic.co/guide/en/elasticsearch/guide/current/pluggable-similarites.html
        builder.startObject("title").field("type", "string")
                .startObject("fields").startObject("shingles").field("type", "string").field("analyzer",
                "my_shingles_analyzer").endObject().endObject()
                .field("similarity", "BM25")
                .startObject("norms").field("enabled", "false").endObject().endObject();
        builder.startObject("description").field("type", "string")
                .startObject("fields").startObject("shingles").field("type", "string").field("analyzer",
                "my_shingles_analyzer").endObject().endObject()
                .field("similarity", "BM25")
                .startObject("norms").field("enabled", "false").endObject().endObject();

        builder.startObject("clicks").field("type", "float").endObject();
        builder.startObject("views").field("type", "float").endObject();
        builder.startObject("impressions").field("type", "float").endObject();

        builder.startObject("status").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("thumbnail_url").field("type", "string").field("index", "no").endObject();
        builder.startObject("resizable_thumbnail_url").field("type", "string").field("index", "no").endObject();
        builder.startObject("channel").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("channel_id").field("type", "string").field("index", "no").endObject();

        builder.startObject("channel_name").field("type", "string").field("index", "no").endObject();
        builder.startObject("channel_url").field("type", "string").field("index", "no").endObject();

        builder.startObject("duration").field("type", "integer").field("index", "no").endObject();
        builder.endObject();

        if (isTtlSet) {
            builder.startObject("_ttl").field("enabled", true).field("default", channelTtl.get()).endObject();
        }

        builder.endObject().endObject();
        return builder;
    }
}
