package com.dailymotion.pixelle.de.processor;

import com.dailymotion.pixelle.common.services.CacheService;
import com.dailymotion.pixelle.de.model.SearchQueryRequest;
import com.dailymotion.pixelle.de.model.Video;
import com.dailymotion.pixelle.de.model.VideoResponse;
import com.dailymotion.pixelle.de.processor.hystrix.ChannelVideoBulkInsertCommand;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicStringProperty;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.Client;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static com.dailymotion.pixelle.de.processor.DeHelper.domain;
import static com.dailymotion.pixelle.de.processor.DeHelper.getDateTimeFormatString;
import static com.dailymotion.pixelle.de.processor.DeHelper.isEmptyList;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.google.common.collect.Ordering.from;
import static com.netflix.config.DynamicPropertyFactory.getInstance;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.util.Collections.sort;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.join;
import static org.apache.commons.lang3.StringUtils.lowerCase;
import static org.joda.time.format.DateTimeFormat.forPattern;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 2/27/15.
 */
public class ChannelProcessor extends VideoProcessor {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final DynamicStringProperty listOfValidCategories = getInstance().getStringProperty("pixelle.channel.categories", "");
    private static final DynamicStringProperty listOfValidSortOrders = getInstance().getStringProperty("pixelle.channel.sortorders", "recent,visited,random");
    private static final DynamicBooleanProperty persistChanneltoES = getInstance().getBooleanProperty("pixelle.channel.es.store", false);
    private static final DynamicIntProperty maxVideosToCache = getInstance().getIntProperty("pixelle.channel" +
            ".maxVideosToCache", 25);
    private static final Logger logger = getLogger(ChannelProcessor.class);

    static {
        OBJECT_MAPPER.configure(FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Inject
    public ChannelProcessor(Client esClient) {
        super(esClient);
    }

    public static List<VideoResponse> recommendChannel(SearchQueryRequest sq, Integer positions) throws Exception {
        String sortOrder = lowerCase(sq.getSortOrder());

        if (isBlank(sortOrder) || !listOfValidSortOrders.get().contains(sortOrder)) {
            sq.setSortOrder("recent"); //default to recent when provided invalid sort order
            logger.info("Invalid sort order provided, defaulting to 'recent' sort order: ", sq.toString());
        }
        /*
         * If videoCache is empty, it is loaded with data from DM - this happens during cold start.
         * If videoCache has non-stale data, it is returned.
         * If videoCache has stale data (6 min since last write), then stale data is returned
         * and fresh data is replaced into the cache asynchronously from DM
         */
        logger.info("getting videos from cache");

        List<VideoResponse> videos = CacheService.getVideos(sq,sortOrder);
        if (videos != null && videos.size() > positions) {
            videos = videos.subList(0, positions);
        }

        logger.info("Num video responses:" + videos.size());
        return videos;
    }

    @Nullable
    private static String getResizableThumbnailUrl(@Nullable String thumbnailUrl) {
        if (isBlank(thumbnailUrl)) {
            return thumbnailUrl;
        }
        // sample url string: http://s1.dmcdn.net/KatPf.jpg
        // id includes id + extension such as this
        String id = thumbnailUrl.substring(thumbnailUrl.lastIndexOf("/") + 1);
        if (isBlank(id)) {
            return null;
        }
        return "//i.".concat(domain.get()).concat("/channel-").concat(id).concat("-thumbnail-1");
    }

    public static void submitAsyncIndexingTask(final List<Video> videos) {
        if (!isEmptyList(videos) && persistChanneltoES.get()) {
            new ChannelVideoBulkInsertCommand(videos).queue();
        }
    }

    public static List<VideoResponse> getFilteredVideos(List<Video> channelVideos) {
        List<VideoResponse> videos = new ArrayList<>();
        for (Video channelVideo : channelVideos) {
            if (!filterVideo(channelVideo)) {
                VideoResponse video = new VideoResponse();

                video.setResizableThumbnailUrl(getResizableThumbnailUrl(channelVideo.getThumbnailUrl()));
                video.setTitle(channelVideo.getTitle());
                video.setDescription(channelVideo.getDescription());
                video.setDuration(channelVideo.getDuration());
                DateTimeFormatter df = forPattern(getDateTimeFormatString());
                DateTime dt = new DateTime(channelVideo.getCreatedTime());

                video.setChannel(channelVideo.getOwnerUsername());
                video.setChannelId(channelVideo.getOwnerId());
                video.setChannelName(channelVideo.getOwnerScreenName());
                video.setVideoId(channelVideo.getVideoIdFromDM());
                videos.add(video);
            }
        }
        if (videos.size() >= maxVideosToCache.get()) {
            return videos.subList(0,maxVideosToCache.get() - 1);
        }
        return videos;
    }

    private static Boolean filterVideo(Video channelVideo) {
        if (!channelVideo.getAllowEmbed()) {
            logger.info("AllowEmbed blocked: " + channelVideo.toString());
            return true;
        }
//        According to publisher biz dev team, we should can show geo blocked videos for channels
//        if (!channelVideo.getGeoBlocking().contains("allow")) {
//            logger.info("Geo blocked: " + channelVideo.toString());
//            return true;
//        }
        if (!isEmptyList(channelVideo.getMediaBlocking())) {
            logger.info("Media blocked: " + channelVideo.toString());
            return true;
        }
        if (!channelVideo.getAds()) {
            logger.info("AdVideo blocked: " + channelVideo.toString());
            return true;
        }
        if (!StringUtils.equalsIgnoreCase(channelVideo.getMode(), "vod")) {
            logger.info("Vod blocked: " + channelVideo.toString());
            return true;
        }
        if (channelVideo.getThreeDim()) {
            logger.info("3Dim blocked: " + channelVideo.toString());
            return true;
        }
        if (channelVideo.getExplicit()) {
            logger.info("Explicit blocked: " + channelVideo.toString());
            return true;
        }
        if (channelVideo.getDuration() < 30) {
            logger.info("Less than 30 blocked: " + channelVideo.toString());
            return true;
        }
        if (!StringUtils.equalsIgnoreCase(channelVideo.getStatus(), "published")) {
            logger.info("Unpublished blocked: " + channelVideo.toString());
            return true;
        }
        return false;
    }
}

