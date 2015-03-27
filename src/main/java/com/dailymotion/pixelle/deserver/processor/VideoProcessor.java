package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.model.Video;
import com.dailymotion.pixelle.deserver.model.VideoResponse;
import com.dailymotion.pixelle.deserver.processor.service.CacheService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicDoubleProperty;
import com.netflix.config.DynamicFloatProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.Explanation;
import org.eclipse.jetty.http.HttpStatus;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by n.dhupia on 12/10/14.
 */
public class VideoProcessor {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String CHANNEL_TIER = "channel_tier";
    private static final String GOLD = "gold";
    private static final String BRONZE = "bronze";
    private static final String SILVER = "silver";
    private static final Logger logger = LoggerFactory.getLogger(VideoProcessor.class);
    private static final DynamicFloatProperty goldPartnerWeight =
            DynamicPropertyFactory.getInstance().getFloatProperty("goldPartner.weightPercent", 0.5f);
    private static final DynamicFloatProperty silverPartnerWeight =
            DynamicPropertyFactory.getInstance().getFloatProperty("silverPartner.weightPercent", 0.25f);
    private static final DynamicFloatProperty maxBoost =
            DynamicPropertyFactory.getInstance().getFloatProperty("maxboost", 10.0f);
    private static final DynamicFloatProperty bronzePartnerWeight =
            DynamicPropertyFactory.getInstance().getFloatProperty("bronzePartner.weightPercent", 0.01f);
    private static final DynamicDoubleProperty pubDateDecay =
            DynamicPropertyFactory.getInstance().getDoubleProperty("publicationDate.decay", 0.25d);
    private static final DynamicStringProperty pubDateScale =
            DynamicPropertyFactory.getInstance().getStringProperty("publicationDate.scale", "180d");
    private static final DynamicStringProperty pubDateOffset =
            DynamicPropertyFactory.getInstance().getStringProperty("publicationDate.offset", "5d");
    private static final DynamicStringProperty scoreMode =
            DynamicPropertyFactory.getInstance().getStringProperty("score.mode", "multiply");
    private static final DynamicStringProperty boostMode =
            DynamicPropertyFactory.getInstance().getStringProperty("boost.mode", "replace");
    private static final DynamicStringProperty ctrScriptFunction =
            DynamicPropertyFactory.getInstance().getStringProperty("ctr.script.code", "");
    private static final DynamicStringProperty ctrScriptLang =
            DynamicPropertyFactory.getInstance().getStringProperty("ctr.script.lang", "groovy");
    private static final DynamicBooleanProperty useVideoCaching =
            DynamicPropertyFactory.getInstance().getBooleanProperty("videoquery.usecache", false);
    static Client client;

    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }


    @Inject
    public VideoProcessor(Client esClient) {
        client = esClient;
    }

    public static Video getVideoById(String id) throws DeException {
        if (StringUtils.isBlank(id)) {
            throw new DeException(new Throwable("id cannot be blank"), HttpStatus.BAD_REQUEST_400);
        }
        GetResponse response = client.prepareGet(DeHelper.organicIndex.get(), DeHelper.videosType.get(), id).execute().actionGet();
        Video video = null;
        byte[] responseSourceAsBytes = response.getSourceAsBytes();
        if (responseSourceAsBytes != null) {
            try {
                video = OBJECT_MAPPER.readValue(responseSourceAsBytes, Video.class);
            } catch (IOException e) {
                logger.error("error parsing video", e);
                throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
            }
        }
        return video;
    }

    /**
     * Modifies video for ES purposes.
     *
     * @param video - should not be null
     * @return a modified video
     */
    private static Video modifyVideoForInsert(@NotNull Video video) {

        if (video == null) {
            return null;
        }

        if (DeHelper.isEmptyList(video.getLanguages())) {
            video.setLanguages(Arrays.asList("all"));
        }

        if (DeHelper.isEmptyList(video.getCategories())) {
            video.setCategories(Arrays.asList("all"));
        }

        video.setCategories(DeHelper.stringListToLowerCase(video.getCategories()));
        video.setLanguages(DeHelper.stringListToLowerCase(video.getLanguages()));

        video.setVideoId(video.getId());
        return video;
    }

    public static void insertVideoInBulk(List<Video> videos) throws DeException {
        insertVideoInBulk(videos, DeHelper.organicIndex.get());
    }

    public static void insertChannelVideoInBulk(List<Video> videos) throws DeException {
        insertVideoInBulk(videos, DeHelper.channelIndex.get());
    }

    private static void insertVideoInBulk(List<Video> videos, String index) {
        if (DeHelper.isEmptyList(videos)) {
            return;
        }
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        logger.info("Bulk loading:" + videos.size() + " videos");
        for (Video video : videos) {
            if (!filterVideo(video)) {
                video = modifyVideoForInsert(video);
                logger.info("Loading video to " + index + " : " + video.toString());

                try {
                    bulkRequest.add(client.prepareUpdate(index, DeHelper.videosType.get(), video.getId())
                            .setDoc(OBJECT_MAPPER.writeValueAsString(video))
                            .setRetryOnConflict(DeHelper.retryOnConflictVideos.get())
                            .setDocAsUpsert(true));
                } catch (JsonProcessingException e) {
                    logger.error("Error converting video to string", e);
                    throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
                }
            }
        }
        if (bulkRequest.numberOfActions() > 0) {
            BulkResponse bulkResponse;
            try {
                bulkResponse = bulkRequest.execute().actionGet();
            } catch (Exception e) {
                throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
            }
            if (bulkResponse != null && bulkResponse.hasFailures()) {
                // process failures by iterating through each bulk response item
                throw new DeException(new Throwable("Error inserting videos in Bulk"), HttpStatus.INTERNAL_SERVER_ERROR_500);
            }
        }
    }

    private static boolean filterVideo(Video video) {
        if (StringUtils.isBlank(video.getResizableThumbnailUrl())) {
            return true;
        }
        return false;
    }

    /**
     * By default, caching is off so units tests can pass. In prod env, we se it to true.
     *
     * @param sq
     * @param positions
     * @return list of videos
     */
    public static List<VideoResponse> recommendUsingCache(@Nullable SearchQueryRequest sq, Integer positions) {

        if (useVideoCaching.get()) {
            try {
                List<VideoResponse> vr = CacheService.getOrganicVideosCache().get(sq);
                logger.info(CacheService.getOrganicVideosCache().stats().toString());
                if (vr.size() > positions) {
                    return vr.subList(0, positions);
                }
                return vr;
            } catch (ExecutionException e) {
                logger.warn("execution exception while getting data form video cache...will send from es directly", e.getCause());
                return recommend(sq, positions);
            }
        }
        return recommend(sq, positions);
    }

    /**
     * Return a list of videos based on search query using elasticsearch.
     *
     * @param sq
     * @param positions
     * @return list of videos.
     */
    public static List<VideoResponse> recommend(@Nullable SearchQueryRequest sq, Integer positions) {

        List<VideoResponse> videoResponses;
        BoolFilterBuilder fb = FilterBuilders.boolFilter();
        if (sq != null) {
            if (!DeHelper.isEmptyList(sq.getCategories())) {
                if (!(sq.getCategories().size() == 1 && sq.getCategories().indexOf("all") == 0)) {
                    fb.must(FilterBuilders.termsFilter("categories", DeHelper.toLowerCase(sq.getCategories())));
                }
            }
            if (!DeHelper.isEmptyList(sq.getLanguages())) {
                fb.must(FilterBuilders.termsFilter("languages", DeHelper.toLowerCase(sq.getLanguages())));
            }
        }
        List<String> excludedIds = sq.getExcludedIds();
        if (!DeHelper.isEmptyList(excludedIds)) {
            for (String id : excludedIds) {
                fb.mustNot(FilterBuilders.termsFilter("video_id", id));
            }
        }

        // origin is current date by default
        ScoreFunctionBuilder pubDateScoreBuilder = ScoreFunctionBuilders
                .gaussDecayFunction("publication_date", pubDateScale.getValue())
                .setDecay(pubDateDecay.getValue())
                .setOffset(pubDateOffset.getValue());

        QueryBuilder qb = QueryBuilders.functionScoreQuery(fb)
                .add(pubDateScoreBuilder)
                .add(FilterBuilders.andFilter(FilterBuilders.rangeFilter("clicks").from(0), FilterBuilders.rangeFilter("impressions").from(0)),
                        ScoreFunctionBuilders.scriptFunction(ctrScriptFunction.getValue()).lang(ctrScriptLang.getValue()))
                .add(FilterBuilders.termFilter(CHANNEL_TIER, GOLD), ScoreFunctionBuilders.weightFactorFunction(goldPartnerWeight.getValue()))
                .add(FilterBuilders.termFilter(CHANNEL_TIER, SILVER), ScoreFunctionBuilders.weightFactorFunction(silverPartnerWeight.getValue()))
                .add(FilterBuilders.termFilter(CHANNEL_TIER, BRONZE), ScoreFunctionBuilders.weightFactorFunction(bronzePartnerWeight.getValue()))
                .add(ScoreFunctionBuilders.randomFunction(sq.getTime()))
                .boostMode(boostMode.getValue())
                .maxBoost(maxBoost.getValue())
                .scoreMode(scoreMode.getValue());

        SearchRequestBuilder srb1 = client.prepareSearch(DeHelper.organicIndex.get())
                .setQuery(qb)
                .setTypes(DeHelper.videosType.get())
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setSize(positions);

        if (sq.isDebugEnabled()) {
            srb1.setExplain(true);
        }
        logger.info(srb1.toString());

        SearchResponse searchResponse = srb1.execute().actionGet();
        videoResponses = new ArrayList<VideoResponse>();

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            try {
                VideoResponse video = OBJECT_MAPPER.readValue(hit.getSourceAsString(), VideoResponse.class);
                if (sq.isDebugEnabled()) {
                    Explanation ex = new Explanation();
                    ex.setValue(hit.getScore());
                    ex.setDescription("Source ====>" + hit.getSourceAsString());
                    ex.addDetail(hit.explanation());
                    video.setDebugInfo(ex.toHtml().replace("\n", ""));
                    logger.info(ex.toString());
                }
                videoResponses.add(video);
            } catch (IOException e) {
                throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
            }
        }
        logger.info("Num video responses:" + videoResponses.size());

        return videoResponses;
    }

    public static List<VideoResponse> getUntargetedVideos(List<VideoResponse> targetedVideo, int positions, SearchQueryRequest sq) {
        logger.warn("Trying to fill query with untargetted videos");
        List<String> languages = sq.getLanguages();
        if (DeHelper.isEmptyList(languages)) {
            languages = Arrays.asList("en"); // default language if none provided
        }
        //exclude the videos we already got
        List<String> excludedIds = new ArrayList<String>();
        for (VideoResponse v : targetedVideo) {
            excludedIds.add(v.getVideoId());
        }

        SearchQueryRequest sq1 = new SearchQueryRequest();
        sq1.setLanguages(languages);
        sq1.setDebugEnabled(sq.isDebugEnabled());
        sq1.setExcludedIds(excludedIds);

        int reqVideosSize = positions;
        if (!DeHelper.isEmptyList(targetedVideo)) {
            reqVideosSize = positions - targetedVideo.size();
        }
        List<VideoResponse> unTargetedVideos = recommend(sq1, reqVideosSize);
        int sizeUnTargeted = 0;
        if (!DeHelper.isEmptyList(unTargetedVideos)) {
            sizeUnTargeted = unTargetedVideos.size();
        }
        if (sizeUnTargeted >= positions) {
            return unTargetedVideos;
        }

        //if we didn't get enough videos for a target lang, we fill it with en lang videos
        if (sizeUnTargeted < reqVideosSize && !languages.contains("en")) {
            sq1.setLanguages(Arrays.asList("en"));

            //make sure to exclude any videos we have in the list already
            for (VideoResponse v : unTargetedVideos) {
                excludedIds.add(v.getVideoId());
            }
            // try to backfill with en videos
            List<VideoResponse> englishTargetedVideos = recommend(sq1, reqVideosSize - sizeUnTargeted);
            int sizeEnglishLangVideos = 0;
            if (!DeHelper.isEmptyList(englishTargetedVideos)) {
                sizeEnglishLangVideos = englishTargetedVideos.size();
            }

            if (sizeUnTargeted > 0 && sizeEnglishLangVideos > 0) {
                unTargetedVideos.addAll(englishTargetedVideos);
                return unTargetedVideos;
            } else if (sizeEnglishLangVideos > 0) {
                return englishTargetedVideos;
            }
        }
        return unTargetedVideos;
    }
}

