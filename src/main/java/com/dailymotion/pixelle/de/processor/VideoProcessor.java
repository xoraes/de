package com.dailymotion.pixelle.de.processor;

import com.dailymotion.pixelle.de.model.SearchQueryRequest;
import com.dailymotion.pixelle.de.model.Video;
import com.dailymotion.pixelle.de.model.VideoResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicDoubleProperty;
import com.netflix.config.DynamicFloatProperty;
import com.netflix.config.DynamicStringProperty;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.dailymotion.pixelle.common.services.CacheService.getOrganicVideosCache;
import static com.dailymotion.pixelle.de.processor.DeHelper.channelIndex;
import static com.dailymotion.pixelle.de.processor.DeHelper.isEmptyList;
import static com.dailymotion.pixelle.de.processor.DeHelper.organicIndex;
import static com.dailymotion.pixelle.de.processor.DeHelper.retryOnConflictVideos;
import static com.dailymotion.pixelle.de.processor.DeHelper.toLowerCase;
import static com.dailymotion.pixelle.de.processor.DeHelper.videosType;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.netflix.config.DynamicPropertyFactory.getInstance;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.eclipse.jetty.http.HttpStatus.BAD_REQUEST_400;
import static org.eclipse.jetty.http.HttpStatus.INTERNAL_SERVER_ERROR_500;
import static org.elasticsearch.action.search.SearchType.QUERY_THEN_FETCH;
import static org.elasticsearch.index.query.FilterBuilders.andFilter;
import static org.elasticsearch.index.query.FilterBuilders.boolFilter;
import static org.elasticsearch.index.query.FilterBuilders.rangeFilter;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.FilterBuilders.termsFilter;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.gaussDecayFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.weightFactorFunction;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 12/10/14.
 */
public class VideoProcessor {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String CHANNEL_TIER = "channel_tier";
    private static final String GOLD = "gold";
    private static final String BRONZE = "bronze";
    private static final String SILVER = "silver";
    private static final Logger logger = getLogger(VideoProcessor.class);
    private static final DynamicFloatProperty goldPartnerWeight =
            getInstance().getFloatProperty("goldPartner.weightPercent", 0.5f);
    private static final DynamicFloatProperty silverPartnerWeight =
            getInstance().getFloatProperty("silverPartner.weightPercent", 0.25f);
    private static final DynamicFloatProperty maxBoost =
            getInstance().getFloatProperty("maxboost", 10.0f);
    private static final DynamicFloatProperty bronzePartnerWeight =
            getInstance().getFloatProperty("bronzePartner.weightPercent", 0.01f);
    private static final DynamicDoubleProperty pubDateDecay =
            getInstance().getDoubleProperty("publicationDate.decay", 0.25d);
    private static final DynamicStringProperty pubDateScale =
            getInstance().getStringProperty("publicationDate.scale", "180d");
    private static final DynamicStringProperty pubDateOffset =
            getInstance().getStringProperty("publicationDate.offset", "5d");
    private static final DynamicStringProperty scoreMode =
            getInstance().getStringProperty("score.mode", "multiply");
    private static final DynamicStringProperty boostMode =
            getInstance().getStringProperty("boost.mode", "replace");
    private static final DynamicStringProperty ctrScriptFunction =
            getInstance().getStringProperty("ctr.script.code", "");
    private static final DynamicStringProperty ctrScriptLang =
            getInstance().getStringProperty("ctr.script.lang", "groovy");
    private static final DynamicBooleanProperty useVideoCaching =
            getInstance().getBooleanProperty("videoquery.usecache", false);
    static Client client;

    static {
        OBJECT_MAPPER.configure(FAIL_ON_UNKNOWN_PROPERTIES, false);
    }


    @Inject
    public VideoProcessor(Client esClient) {
        client = esClient;
    }

    public static Video getVideoById(String id) throws DeException {
        if (isBlank(id)) {
            throw new DeException(new Throwable("id cannot be blank"), BAD_REQUEST_400);
        }
        GetResponse response = client.prepareGet(organicIndex.get(), videosType.get(), id).execute().actionGet();
        Video video = null;
        byte[] responseSourceAsBytes = response.getSourceAsBytes();
        if (responseSourceAsBytes != null) {
            try {
                video = OBJECT_MAPPER.readValue(responseSourceAsBytes, Video.class);
            } catch (IOException e) {
                logger.error("error parsing video", e);
                throw new DeException(e, INTERNAL_SERVER_ERROR_500);
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

        if (isEmptyList(video.getLanguages())) {
            video.setLanguages(asList("all"));
        }

        if (isEmptyList(video.getCategories())) {
            video.setCategories(asList("all"));
        }

        video.setCategories(toLowerCase(video.getCategories()));
        video.setLanguages(toLowerCase(video.getLanguages()));

        video.setVideoId(video.getId());
        return video;
    }

    public static void insertVideoInBulk(List<Video> videos) throws DeException {
        insertVideoInBulk(videos, organicIndex.get());
    }

    public static void insertChannelVideoInBulk(List<Video> videos) throws DeException {
        insertVideoInBulk(videos, channelIndex.get());
    }

    private static void insertVideoInBulk(List<Video> videos, String index) throws DeException {
        if (isEmptyList(videos)) {
            return;
        }
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        logger.info("Bulk loading:" + videos.size() + " videos");
        for (Video video : videos) {
            if (!filterVideo(video)) {
                video = modifyVideoForInsert(video);
                logger.info("Loading video to " + index + " : " + video.toString());

                try {
                    bulkRequest.add(client.prepareUpdate(index, videosType.get(), video.getId())
                            .setDoc(OBJECT_MAPPER.writeValueAsString(video))
                            .setRetryOnConflict(retryOnConflictVideos.get())
                            .setDocAsUpsert(true));
                } catch (JsonProcessingException e) {
                    logger.error("Error converting video to string", e);
                    throw new DeException(e, INTERNAL_SERVER_ERROR_500);
                }
            }
        }
        if (bulkRequest.numberOfActions() > 0) {
            BulkResponse bulkResponse;
            try {
                bulkResponse = bulkRequest.execute().actionGet();
            } catch (Exception e) {
                throw new DeException(e, INTERNAL_SERVER_ERROR_500);
            }
            if (bulkResponse != null && bulkResponse.hasFailures()) {
                // process failures by iterating through each bulk response item
                throw new DeException(new Throwable("Error inserting videos in Bulk"), INTERNAL_SERVER_ERROR_500);
            }
        }
    }

    private static boolean filterVideo(Video video) {
        return isBlank(video.getResizableThumbnailUrl());
    }

    /**
     * By default, caching is off so units tests can pass. In prod env, we se it to true.
     *
     * @param sq
     * @param positions
     * @return list of videos
     */
    public static List<VideoResponse> recommendUsingCache(@Nullable SearchQueryRequest sq, Integer positions) throws DeException {

        if (useVideoCaching.get()) {
            try {
                List<VideoResponse> vr = getOrganicVideosCache().get(sq);
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
    public static List<VideoResponse> recommend(@Nullable SearchQueryRequest sq, Integer positions) throws DeException {

        List<VideoResponse> videoResponses;
        BoolFilterBuilder fb = boolFilter();
        if (sq != null) {
            if (!isEmptyList(sq.getCategories()) && !(sq.getCategories().size() == 1 && sq.getCategories().indexOf("all") == 0)) {
                fb.must(termsFilter("categories", toLowerCase(sq.getCategories())));
            }
            if (!isEmptyList(sq.getLanguages())) {
                fb.must(termsFilter("languages", toLowerCase(sq.getLanguages())));
            }
        }
        List<String> excludedIds = sq.getExcludedVideoIds();
        if (!isEmptyList(excludedIds)) {
            for (String id : excludedIds) {
                fb.mustNot(termsFilter("video_id", id));
            }
        }

        // origin is current date by default
        ScoreFunctionBuilder pubDateScoreBuilder =
                gaussDecayFunction("publication_date", pubDateScale.getValue())
                        .setDecay(pubDateDecay.getValue())
                        .setOffset(pubDateOffset.getValue());

        QueryBuilder qb = functionScoreQuery(fb)
                .add(pubDateScoreBuilder)
                .add(andFilter(rangeFilter("clicks").from(0), rangeFilter("impressions").from(0)),
                        scriptFunction(ctrScriptFunction.getValue()).lang(ctrScriptLang.getValue()))
                .add(termFilter(CHANNEL_TIER, GOLD), weightFactorFunction(goldPartnerWeight.getValue()))
                .add(termFilter(CHANNEL_TIER, SILVER), weightFactorFunction(silverPartnerWeight.getValue()))
                .add(termFilter(CHANNEL_TIER, BRONZE), weightFactorFunction(bronzePartnerWeight.getValue()))
                .boostMode(boostMode.getValue())
                .maxBoost(maxBoost.getValue())
                .scoreMode(scoreMode.getValue());

        SearchRequestBuilder srb1 = client.prepareSearch(organicIndex.get())
                .setQuery(qb)
                .setTypes(videosType.get())
                .setSearchType(QUERY_THEN_FETCH)
                .setSize(positions);

        if (sq.isDebugEnabled()) {
            srb1.setExplain(true);
        }
        logger.info(srb1.toString());
        SearchResponse searchResponse = null;
        try {
            searchResponse = srb1.execute().actionGet();
        } catch (ElasticsearchException e) {
            throw new DeException(e, INTERNAL_SERVER_ERROR_500);
        }
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
                throw new DeException(e, INTERNAL_SERVER_ERROR_500);
            }
        }
        logger.info("Num video responses:" + videoResponses.size());

        return videoResponses;
    }

    public static List<VideoResponse> getUntargetedVideos(List<VideoResponse> targetedVideo, int positions, SearchQueryRequest sq) throws DeException {
        logger.info("Trying to fill query with untargetted videos: Search Query" + sq.toString());
        List<String> languages = sq.getLanguages();
        if (isEmptyList(languages)) {
            languages = asList("en"); // default language if none provided
        }
        //exclude the videos we already got
        List<String> excludedIds = new ArrayList<String>();
        for (VideoResponse v : targetedVideo) {
            excludedIds.add(v.getVideoId());
        }

        SearchQueryRequest sq1 = new SearchQueryRequest();
        sq1.setLanguages(languages);
        sq1.setDebugEnabled(sq.isDebugEnabled());
        sq1.setExcludedVideoIds(excludedIds);

        int reqVideosSize = positions;
        if (!isEmptyList(targetedVideo)) {
            reqVideosSize = positions - targetedVideo.size();
        }
        List<VideoResponse> unTargetedVideos = recommend(sq1, reqVideosSize);
        int sizeUnTargeted = 0;
        if (!isEmptyList(unTargetedVideos)) {
            sizeUnTargeted = unTargetedVideos.size();
        }
        if (sizeUnTargeted >= positions) {
            return unTargetedVideos;
        }

        //if we didn't get enough videos for a target lang, we fill it with en lang videos
        if (sizeUnTargeted < reqVideosSize && !languages.contains("en")) {
            sq1.setLanguages(asList("en"));

            //make sure to exclude any videos we have in the list already
            for (VideoResponse v : unTargetedVideos) {
                excludedIds.add(v.getVideoId());
            }
            // try to backfill with en videos
            List<VideoResponse> englishTargetedVideos = recommend(sq1, reqVideosSize - sizeUnTargeted);
            int sizeEnglishLangVideos = 0;
            if (!isEmptyList(englishTargetedVideos)) {
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

