package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.ChannelVideo;
import com.dailymotion.pixelle.deserver.model.ChannelVideos;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.model.Video;
import com.dailymotion.pixelle.deserver.model.VideoResponse;
import com.dailymotion.pixelle.deserver.processor.hystrix.ChannelVideoBulkInsertCommand;
import com.dailymotion.pixelle.deserver.processor.service.CacheService;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by n.dhupia on 2/27/15.
 */
public class ChannelProcessor extends VideoProcessor {
    private static final Integer MAX_RANDOM = 100;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final DynamicStringProperty listOfValidCategories = DynamicPropertyFactory.getInstance().getStringProperty("pixelle.channel.categories", "");
    private static final DynamicBooleanProperty persistChanneltoES = DynamicPropertyFactory.getInstance().getBooleanProperty("pixelle.channel.es.store", false);
    private static final Logger logger = LoggerFactory.getLogger(ChannelProcessor.class);

    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Inject
    public ChannelProcessor(Client esClient) {
        super(esClient);
    }

    public static List<VideoResponse> recommendChannel(SearchQueryRequest sq, Integer positions) throws Exception {

        List<VideoResponse> videoResponses = null;


        BoolFilterBuilder fb = FilterBuilders.boolFilter();
        fb.must(FilterBuilders.termFilter("channel", sq.getChannel()));

        QueryBuilder qb = QueryBuilders.functionScoreQuery(fb)
                .add(ScoreFunctionBuilders.randomFunction((int) (Math.random() * MAX_RANDOM)));

        SearchRequestBuilder srb1 = client.prepareSearch(DeHelper.channelIndex.get())
                .setTypes(DeHelper.videosType.get())
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(qb)
                .setSize(positions);

        logger.info(srb1.toString());

        SearchResponse searchResponse = srb1.execute().actionGet();
        videoResponses = new ArrayList<>();

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            VideoResponse videoResponse = OBJECT_MAPPER.readValue(hit.getSourceAsString(), VideoResponse.class);
            videoResponses.add(videoResponse);
        }
        /*
         * User request first hits ES. If num positions are found, then done. Otherwise, look in the videoCache.
         * If videoCache is empty, it is loaded with data from DM - this happens during cold start.
         * If videoCache has non-stale data, it is returned.
         * If videoCache has stale data (6 min since last write), then stale data is returned
         * and fresh data is replaced into the cache asynchronously from DM and indexed to ES.
         */
        if (DeHelper.isEmptyList(videoResponses) || videoResponses.size() < positions) {
            videoResponses = new ArrayList<>();
            List<Video> videos = null;
            logger.info("getting videos from cache");
            LoadingCache<String, List<Video>> cache = CacheService.getChannelVideosCache();
            if (cache != null) {
                videos = cache.get(sq.getChannel());
            }
            if (videos != null) {
                int numVideos = videos.size();
                if (numVideos > positions) {
                    numVideos = positions;
                }
                for (int i = 0; i < numVideos; i++) {
                    VideoResponse videoResponse = new VideoResponse();
                    videoResponse.setChannel(videos.get(i).getChannel());
                    videoResponse.setChannelId(videos.get(i).getChannelId());
                    videoResponse.setChannelName(videos.get(i).getChannelName());
                    videoResponse.setDescription(videos.get(i).getDescription());
                    videoResponse.setTitle(videos.get(i).getTitle());
                    videoResponse.setResizableThumbnailUrl(videos.get(i).getResizableThumbnailUrl());
                    videoResponse.setDuration(videos.get(i).getDuration());
                    videoResponse.setVideoId(videos.get(i).getVideoId());
                    videoResponses.add(videoResponse);
                }
            }
        }
        logger.info("Num video responses:" + videoResponses.size());
        return videoResponses;
    }

    @Nullable
    private static String getResizableThumbnailUrl(@Nullable String thumbnailUrl) {
        if (StringUtils.isBlank(thumbnailUrl)) {
            return thumbnailUrl;
        }
        // sample url string: http://s1.dmcdn.net/KatPf.jpg
        // id includes id + extension such as this
        String id = thumbnailUrl.substring(thumbnailUrl.lastIndexOf("/") + 1);
        if (StringUtils.isBlank(id)) {
            return null;
        }
        return "//i.".concat(DeHelper.domain.get()).concat("/channel-").concat(id).concat("-thumbnail-1");
    }

    public static void submitAsyncIndexingTask(final List<Video> videos) {
        if (!DeHelper.isEmptyList(videos) && persistChanneltoES.get()) {
            new ChannelVideoBulkInsertCommand(videos).queue();
        }
    }

    public static List<Video> getFilteredVideos(ChannelVideos channelVideos) {
        List<Video> videos = new ArrayList<>();
        for (ChannelVideo channelVideo : channelVideos.getList()) {
            if (!filterVideo(channelVideo)) {
                Video video = new Video();
                video.setLanguages(Arrays.asList(channelVideo.getLanguage()));
                video.setResizableThumbnailUrl(getResizableThumbnailUrl(channelVideo.getThumbnailUrl()));
                video.setTitle(channelVideo.getTitle());
                video.setDescription(channelVideo.getDescription());
                video.setDuration(channelVideo.getDuration());
                DateTimeFormatter df = DateTimeFormat.forPattern(DeHelper.getDateTimeFormatString());
                DateTime dt = new DateTime(channelVideo.getCreatedTime());
                video.setPublicationDate(dt.toString(df));
                video.setChannel(channelVideo.getOwnerUsername());
                video.setChannelId(channelVideo.getOwnerId());
                video.setChannelName(channelVideo.getOwnerScreenName());
                video.setCategories(Arrays.asList(channelVideo.getChannel()));
                video.setTags(channelVideo.getTags());
                video.setId(channelVideo.getVideoId());
                video.setVideoId(channelVideo.getVideoId());
                videos.add(video);
            }
        }
        return videos;
    }

    private static Boolean filterVideo(ChannelVideo channelVideo) {
        if (!channelVideo.getAllowEmbed()) {
            return true;
        }
        if (!channelVideo.getGeoBlocking().contains("allow")) {
            return true;
        }
        if (!DeHelper.isEmptyList(channelVideo.getMediaBlocking())) {
            return true;
        }
        if (!channelVideo.getAds()) {
            return true;
        }
        if (!channelVideo.getMode().equalsIgnoreCase("vod")) {
            return true;
        }
        if (channelVideo.getThreeDim()) {
            return true;
        }
        if (channelVideo.getExplicit()) {
            return true;
        }
        if (channelVideo.getDuration() < 30) {
            return true;
        }
        if (!channelVideo.getStatus().equalsIgnoreCase("published")) {
            return true;
        }
        return !listOfValidCategories.get().contains(channelVideo.getChannel().toLowerCase());
    }
}

