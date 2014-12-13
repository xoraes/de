package com.dailymotion.pixelle.deserver.providers;

import com.dailymotion.pixelle.deserver.processor.DeException;
import com.dailymotion.pixelle.deserver.processor.DeHelper;
import com.google.inject.Injector;
import com.google.inject.ProvisionException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
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
public class ESIndexTypeFactory {
    private static Logger logger = LoggerFactory.getLogger(ESIndexTypeFactory.class);
    private static Injector injector;

    public static void createIndex(Client client, String indexName, Settings settings, String... types) throws DeException {
        boolean ack = false;
        try {
            // create the index if it does not already exist. If it exists don't do anything.

            if (!client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists()) {
                ack = client.admin().indices().create(new CreateIndexRequest(indexName, settings)).actionGet().isAcknowledged();
                if (ack == true) {
                    logger.info("Index creation succeeded");
                } else {
                    logger.info("Index already exists ! Not re-creating");
                }
            }
            for (String typeName : types) {
                if (!client.admin().indices().typesExists(new TypesExistsRequest(new String[]{indexName}, typeName))
                        .actionGet().isExists()) {

                    ack = client.admin().indices()
                            .putMapping(new PutMappingRequest(indexName).type(typeName).source(createMapping(typeName)))
                            .actionGet().isAcknowledged();

                    if (ack) {
                        logger.info("type creation succeeded");
                    } else {
                        logger.info("type already exists ! Not re-creating");
                    }
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
            //TODO Use throwableProvider instead of this
            throw new ProvisionException("Could not create index", e);
        }
    }

    private static XContentBuilder createMapping(String typeName) throws IOException {
        if (typeName.equalsIgnoreCase(DeHelper.getAdUnitsType())) {
            return createAdUnitMapping(typeName);
        } else {
            return createVideosMapping(typeName);
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

        builder.startObject("tactic").field("type", "string").field("index", "no").endObject();
        builder.startObject("title").field("type", "string").field("index", "no").endObject();
        builder.startObject("description").field("type", "string").field("index", "no").endObject();
        builder.startObject("thumbnail_url").field("type", "string").field("index", "no").endObject();
        builder.startObject("resizable_thumbnail_url").field("type", "string").field("index", "no").endObject();
        builder.startObject("video_url").field("type", "string").field("index", "no").endObject();
        builder.startObject("video_id").field("type", "string").field("index", "no").endObject();
        builder.startObject("channel").field("type", "string").field("index", "no").endObject();
        builder.startObject("channel_url").field("type", "string").field("index", "no").endObject();
        builder.startObject("goal_period").field("type", "string").field("index", "no").endObject();
        builder.startObject("account").field("type", "string").field("index", "no").endObject();
        builder.startObject("delivery").field("type", "string").field("index", "no").endObject();

        builder.startObject("duration").field("type", "integer").field("index", "no").endObject();

        builder.startObject("goal_views").field("type", "float").field("index", "no").endObject();
        builder.startObject("clicks").field("type", "float").field("index", "no").endObject();
        builder.startObject("views").field("type", "float").field("index", "no").endObject();

        builder.startObject("goal_reached").field("type", "boolean").field("index", "not_analyzed").endObject();
        builder.startObject("paused").field("type", "boolean").field("index", "not_analyzed").endObject();

        builder.endObject().endObject().endObject();


        return builder;
    }

    private static XContentBuilder createVideosMapping(String typeName) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject(typeName).startObject("properties");
        builder.startObject("_created").field("type", "date").field("format", "date_time_no_millis").field("index", "not_analyzed").endObject();
        builder.startObject("_updated").field("type", "date").field("format", "date_time_no_millis").field("index", "not_analyzed").endObject();
        builder.startObject("_id").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("video_id").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("languages").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("categories").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("tags").field("type", "string").field("index", "not_analyzed").endObject();

        builder.startObject("status").field("type", "string").field("index", "not_analyzed").endObject();
        builder.startObject("title").field("type", "string").field("index", "no").endObject();
        builder.startObject("description").field("type", "string").field("index", "no").endObject();
        builder.startObject("thumbnail_url").field("type", "string").field("index", "no").endObject();
        builder.startObject("resizable_thumbnail_url").field("type", "string").field("index", "no").endObject();
        builder.startObject("channel").field("type", "string").field("index", "no").endObject();
        builder.startObject("channel_id").field("type", "string").field("index", "no").endObject();
        builder.startObject("channel_tier").field("type", "string").field("index", "no").endObject();

        builder.startObject("duration").field("type", "integer").field("index", "no").endObject();

        builder.endObject().endObject().endObject();


        return builder;
    }
}
