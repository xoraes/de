package com.dailymotion.pixelle.deserver.providers;

import com.dailymotion.pixelle.deserver.processor.DeException;
import com.dailymotion.pixelle.deserver.processor.DeHelper;
import com.google.inject.ProvisionException;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by n.dhupia on 11/19/14.
 */
final class ESIndexTypeFactory {
    private static final Logger logger = LoggerFactory.getLogger(ESIndexTypeFactory.class);
    private static final DynamicIntProperty channelTtl =
            DynamicPropertyFactory.getInstance().getIntProperty("channel.index.ttl", 300000); // 5 minutes


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
        if (indexName.equalsIgnoreCase(DeHelper.promotedIndex.get())) {
            return createAdUnitMapping(DeHelper.adunitsType.get());
        } else if (indexName.equalsIgnoreCase(DeHelper.organicIndex.get())) {
            return createVideosMapping(DeHelper.videosType.get());
        } else {
            return createVideosMapping(DeHelper.videosType.get(), true);
        }
    }


    private static XContentBuilder createAdUnitMapping(String typeName) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject(typeName).startObject("properties");
        builder.startObject("_created").field("type", "date").field("format", "date_time_no_millis").field("index", "not_analyzed").endObject();
        builder.startObject("_updated").field("type", "date").field("format", "date_time_no_millis").field("index", "not_analyzed").endObject();
        builder.startObject("start_date").field("type", "date").field("format", "date_time_no_millis").field("index", "not_analyzed").endObject();
        builder.startObject("end_date").field("type", "date").field("format", "date_time_no_millis").field("index", "not_analyzed").endObject();

        builder.startObject("_id").field("type", "string").field("index", "not_analyzed").endObject();
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

        builder.startObject("cpc").field("type", "integer").endObject();
        builder.startObject("cpv").field("type", "integer").endObject();

        builder.startObject("clicks").field("type", "float").endObject();
        builder.startObject("views").field("type", "float").endObject();
        builder.startObject("impressions").field("type", "float").endObject();
        builder.startObject("goal_views").field("type", "float").endObject();

        builder.startObject("tactic").field("type", "string").field("index", "no").endObject();
        builder.startObject("title").field("type", "string").field("index", "no").endObject();
        builder.startObject("description").field("type", "string").field("index", "no").endObject();
        builder.startObject("thumbnail_url").field("type", "string").field("index", "no").endObject();
        builder.startObject("resizable_thumbnail_url").field("type", "string").field("index", "no").endObject();
        builder.startObject("video_url").field("type", "string").field("index", "no").endObject();
        builder.startObject("video_id").field("type", "string").field("index", "no").endObject();
        builder.startObject("channel").field("type", "string").field("index", "no").endObject();
        builder.startObject("channel_id").field("type", "string").field("index", "no").endObject();
        builder.startObject("channel_name").field("type", "string").field("index", "no").endObject();
        builder.startObject("channel_url").field("type", "string").field("index", "no").endObject();
        builder.startObject("goal_period").field("type", "string").field("index", "no").endObject();
        builder.startObject("account").field("type", "string").field("index", "no").endObject();
        builder.startObject("delivery").field("type", "string").field("index", "no").endObject();


        builder.startObject("duration").field("type", "integer").field("index", "no").endObject();

        builder.startObject("paused").field("type", "boolean").field("index", "not_analyzed").endObject();

        builder.endObject().endObject().endObject();


        return builder;
    }

    private static XContentBuilder createVideosMapping(String typeName) throws IOException {
        return createVideosMapping(typeName, false);
    }

    private static XContentBuilder createVideosMapping(String typeName, Boolean isTtlSet) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject(typeName).startObject("properties");
        builder.startObject("_created").field("type", "date").field("format", "date_time_no_millis").field("index", "not_analyzed").endObject();
        builder.startObject("_updated").field("type", "date").field("format", "date_time_no_millis").field("index", "not_analyzed").endObject();
        builder.startObject("publication_date").field("type", "date").field("format", "date_time_no_millis").field("index", "not_analyzed").endObject();

        builder.startObject("_id").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("video_id").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("languages").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("categories").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("tags").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("channel_tier").field("type", "string").field("index", "not_analyzed").endObject();

        builder.startObject("clicks").field("type", "float").endObject();
        builder.startObject("views").field("type", "float").endObject();
        builder.startObject("impressions").field("type", "float").endObject();

        builder.startObject("status").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("title").field("type", "string").field("index", "no").endObject();
        builder.startObject("description").field("type", "string").field("index", "no").endObject();
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
