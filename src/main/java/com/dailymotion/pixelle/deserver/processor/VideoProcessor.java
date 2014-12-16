package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.model.Video;
import com.dailymotion.pixelle.deserver.model.VideoResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.get.GetResponse;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by n.dhupia on 12/10/14.
 */
public class VideoProcessor {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private static Logger logger = LoggerFactory.getLogger(VideoProcessor.class);
    private static Client client;

    @Inject
    public VideoProcessor(Client esClient) {
        this.client = esClient;
    }

    public Video getVideoById(String id) throws DeException {
        if (StringUtils.isBlank(id)) {
            throw new DeException(new Throwable("id cannot be blank"), 400);
        }
        GetResponse response = client.prepareGet(DeHelper.getIndex(), DeHelper.getOrganicVideoType(), id).execute().actionGet();
        Video video = null;
        byte[] responseSourceAsBytes = response.getSourceAsBytes();
        if (response != null && responseSourceAsBytes != null) {
            try {
                video = objectMapper.readValue(responseSourceAsBytes, Video.class);
            } catch (IOException e) {
                logger.error("error parsing video", e);
                throw new DeException(e, 500);
            }
        }
        return video;
    }

    public boolean insertVideo(Video video) throws DeException {

        if (video == null) {
            throw new DeException(new Throwable("no video found in request body"), 400);
        }

        if (video.getLanguages() == null || video.getLanguages().size() <= 0) {
            video.setLanguages(Arrays.asList("all"));
        }


        if (video.getCategories() == null || video.getCategories().size() <= 0) {
            video.setCategories(Arrays.asList("all"));
        }

        video.setCategories(DeHelper.stringListToLowerCase(video.getCategories()));
        video.setLanguages(DeHelper.stringListToLowerCase(video.getLanguages()));

        video.setStatus(StringUtils.lowerCase(video.getStatus()));
        video.setVideoId(video.getId());
        return updateVideo(video);
    }

    public boolean updateVideo(Video video) throws DeException {
        if (video == null) {
            throw new DeException(new Throwable("no video found in request body"), 400);
        }
        logger.info(video.toString());

        boolean result = false;
        try {
            result = client.prepareUpdate(DeHelper.getIndex(), DeHelper.getOrganicVideoType(), video.getId())
                    .setDoc(objectMapper.writeValueAsString(video))
                    .setDocAsUpsert(true)
                    .execute()
                    .actionGet()
                    .isCreated();

        } catch (JsonProcessingException e) {
            logger.error("Error converting video to string", e);
            throw new DeException(e, 500);
        }
        return result;
    }

    public List<VideoResponse> recommend(@Nullable SearchQueryRequest sq, Integer positions, @Nullable List<String> excludedIds) {
        List<VideoResponse> videoResponses = null;
        BoolFilterBuilder fb = FilterBuilders.boolFilter();
        fb.must(FilterBuilders.termFilter("status", "active"));
        if (sq != null) {
            fb.must(FilterBuilders.termsFilter("categories", DeHelper.toLowerCase(sq.getCategories())));
            fb.must(FilterBuilders.termsFilter("languages", DeHelper.toLowerCase(sq.getLanguages())));
        } else if (!DeHelper.isEmptyArray(excludedIds)) {
            for (String id : excludedIds) {
                fb.mustNot(FilterBuilders.termsFilter("video_id", id));
            }
        }
        QueryBuilder qb = QueryBuilders.functionScoreQuery(fb)
                .add(ScoreFunctionBuilders.randomFunction((int) (Math.random() * 100)));
        SearchRequestBuilder srb1 = client.prepareSearch(DeHelper.getIndex())
                .setTypes(DeHelper.getOrganicVideoType())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb)
                .setSize(positions);


        logger.info(srb1.toString());

        SearchResponse searchResponse = srb1.execute().actionGet();
        videoResponses = new ArrayList<VideoResponse>();

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            VideoResponse video;
            try {
                video = objectMapper.readValue(hit.getSourceAsString(), VideoResponse.class);
            } catch (IOException e) {
                throw new DeException(e.getCause(), 500);
            }
            videoResponses.add(video);
        }
        logger.info("Num video responses:" + videoResponses.size());


        return videoResponses;
    }

    public List<VideoResponse> getDistinctUntargetedVideo(List<VideoResponse> targetedVideo, int positions) {
        List<String> excludedIds = new ArrayList<String>();
        for (VideoResponse v : targetedVideo) {
            excludedIds.add(v.getVideoId());
        }
        return recommend(null, positions, excludedIds);
    }

}

