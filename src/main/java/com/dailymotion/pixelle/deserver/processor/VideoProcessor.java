package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.model.Video;
import com.dailymotion.pixelle.deserver.model.VideoResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.netflix.config.DynamicDoubleProperty;
import com.netflix.config.DynamicFloatProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.Explanation;
import org.eclipse.jetty.http.HttpStatus;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
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

/**
 * Created by n.dhupia on 12/10/14.
 */
public class VideoProcessor {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private static final String CHANNEL_TIER = "channel_tier";
    private static final String GOLD = "gold";
    private static final String BRONZE = "bronze";
    private static final String SILVER = "silver";
    protected static Client client;
    private static Logger logger = LoggerFactory.getLogger(VideoProcessor.class);
    private static DynamicFloatProperty goldPartnerWeight =
            DynamicPropertyFactory.getInstance().getFloatProperty("goldPartner.weightPercent", 0.5f);

    private static DynamicFloatProperty silverPartnerWeight =
            DynamicPropertyFactory.getInstance().getFloatProperty("silverPartner.weightPercent", 0.25f);

    private static DynamicFloatProperty maxBoost =
            DynamicPropertyFactory.getInstance().getFloatProperty("maxboost", 10.0f);

    private static DynamicFloatProperty bronzePartnerWeight =
            DynamicPropertyFactory.getInstance().getFloatProperty("bronzePartner.weightPercent", 0.01f);

    private static DynamicDoubleProperty pubDateDecay =
            DynamicPropertyFactory.getInstance().getDoubleProperty("publicationDate.decay", 0.25d);

    private static DynamicStringProperty pubDateScale =
            DynamicPropertyFactory.getInstance().getStringProperty("publicationDate.scale", "180d");

    private static DynamicStringProperty pubDateOffset =
            DynamicPropertyFactory.getInstance().getStringProperty("publicationDate.offset", "5d");

    private static DynamicStringProperty scoreMode =
            DynamicPropertyFactory.getInstance().getStringProperty("score.mode", "multiply");

    private static DynamicStringProperty boostMode =
            DynamicPropertyFactory.getInstance().getStringProperty("boost.mode", "replace");

    private static DynamicStringProperty ctrScriptFunction =
            DynamicPropertyFactory.getInstance().getStringProperty("ctr.script.code", "");
    private static DynamicStringProperty ctrScriptLang =
            DynamicPropertyFactory.getInstance().getStringProperty("ctr.script.lang", "groovy");

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
        if (response != null && responseSourceAsBytes != null) {
            try {
                video = OBJECT_MAPPER.readValue(responseSourceAsBytes, Video.class);
            } catch (IOException e) {
                logger.error("error parsing video", e);
                throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
            }
        }
        return video;
    }

    public static void insertVideo(Video video) throws DeException {

        if (video == null) {
            throw new DeException(new Throwable("no video found in request body"), HttpStatus.BAD_REQUEST_400);
        }
        updateVideo(modifyVideoForInsert(video));
    }

    /**
     * @param video - should not be null
     * @return a modified video
     */
    protected static Video modifyVideoForInsert(@NotNull Video video) {

        if (video == null) return video;

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

    public static void updateVideo(Video video) throws DeException {
        if (video == null) {
            throw new DeException(new Throwable("no video found in request body"), HttpStatus.BAD_REQUEST_400);
        }
        logger.info(video.toString());
        UpdateResponse response;
        try {
            response = client.prepareUpdate(DeHelper.organicIndex.get(), DeHelper.videosType.get(), video.getId())
                    .setDoc(OBJECT_MAPPER.writeValueAsString(video))
                    .setDocAsUpsert(true)
                    .setRetryOnConflict(DeHelper.retryOnConflictVideos.get())
                    .execute()
                    .actionGet();


        } catch (JsonProcessingException e) {
            logger.error("Error converting video to string", e);
            throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
        } catch (ElasticsearchException e) {
            throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
        }
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

    public static List<VideoResponse> recommend(@Nullable SearchQueryRequest sq, Integer positions, @Nullable List<String> excludedIds) {

        List<VideoResponse> videoResponses = null;
        BoolFilterBuilder fb = FilterBuilders.boolFilter();
        if (sq != null) {
            if (!DeHelper.isEmptyList(sq.getCategories())) {
                fb.must(FilterBuilders.termsFilter("categories", DeHelper.toLowerCase(sq.getCategories())));
            }
            if (!DeHelper.isEmptyList(sq.getLanguages())) {
                fb.must(FilterBuilders.termsFilter("languages", DeHelper.toLowerCase(sq.getLanguages())));
            }
        }
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
                .setTypes(DeHelper.videosType.get())
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(qb)
                .setExplain(sq.getDebugEnabled())
                .setSize(positions);

        logger.info(ctrScriptFunction.getName() + " : " + ctrScriptFunction.getValue());
        logger.info(ctrScriptLang.getName() + " : " + ctrScriptLang.getValue());
        logger.info(goldPartnerWeight.getName() + " : " + goldPartnerWeight.getValue());
        logger.info(silverPartnerWeight.getName() + " : " + silverPartnerWeight.getValue());
        logger.info(bronzePartnerWeight.getName() + " : " + bronzePartnerWeight.getValue());
        logger.info(boostMode.getName() + " : " + boostMode.getValue());
        logger.info(scoreMode.getName() + " : " + scoreMode.getValue());
        logger.info(maxBoost.getName() + " : " + maxBoost.getValue());
        logger.info(srb1.toString());

        SearchResponse searchResponse = srb1.execute().actionGet();
        videoResponses = new ArrayList<VideoResponse>();

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            try {
                VideoResponse video = OBJECT_MAPPER.readValue(hit.getSourceAsString(), VideoResponse.class);
                if (sq.getDebugEnabled() == Boolean.TRUE) {
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
        sq1.setDebugEnabled(sq.getDebugEnabled());

        int reqVideosSize = positions;
        if (!DeHelper.isEmptyList(targetedVideo)) {
            reqVideosSize = positions - targetedVideo.size();
        }
        List<VideoResponse> unTargetedVideos = recommend(sq1, reqVideosSize, excludedIds);
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
            List<VideoResponse> englishTargetedVideos = recommend(sq1, reqVideosSize - sizeUnTargeted, excludedIds);
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

