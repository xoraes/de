package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.ChannelVideo;
import com.dailymotion.pixelle.deserver.model.ChannelVideos;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.model.Video;
import com.dailymotion.pixelle.deserver.model.VideoResponse;
import com.dailymotion.pixelle.deserver.processor.hystrix.ChannelVideoBulkInsertCommand;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.eclipse.jetty.http.HttpStatus;
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

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by n.dhupia on 2/27/15.
 */
public class ChannelProcessor extends VideoProcessor {
    private static final Integer MAX_RANDOM = 100;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final DynamicStringProperty dmUserVideoUrl = DynamicPropertyFactory.getInstance().getStringProperty("dm.video.url", "");
    private static final DynamicStringProperty listOfValidCategories = DynamicPropertyFactory.getInstance().getStringProperty("pixelle.channel.categories", "");
    private static Logger logger = LoggerFactory.getLogger(ChannelProcessor.class);
    private static CloseableHttpClient httpclient = HttpClients.createDefault();


    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Inject
    public ChannelProcessor(Client esClient) {
        super(esClient);
    }

    public List<VideoResponse> recommend(SearchQueryRequest sq, Integer positions) throws DeException {

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
        videoResponses = new ArrayList<VideoResponse>();

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            try {
                VideoResponse video = OBJECT_MAPPER.readValue(hit.getSourceAsString(), VideoResponse.class);
                videoResponses.add(video);
            } catch (IOException e) {
                throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
            }
        }
        logger.info("Num video responses:" + videoResponses.size());
        logger.info("Num video responses:" + searchResponse.getHits().getHits().length);

        if (DeHelper.isEmptyList(videoResponses)) {
            List<Video> videos = getVideosFromDM(sq.getChannel());

            int numVideos = videos.size();
            if (numVideos > positions) {
                numVideos = positions;
            }
            for (int i = 0; i < numVideos; i++) {
                VideoResponse videoResponse = new VideoResponse();
                videoResponse.setChannel(videos.get(i).getChannel());
                videoResponse.setChannelId(videos.get(i).getChannelId());
                videoResponse.setDescription(videos.get(i).getDescription());
                videoResponse.setTitle(videos.get(i).getTitle());
                videoResponse.setThumbnailUrl(videos.get(i).getThumbnailUrl());
                videoResponse.setDuration(videos.get(i).getDuration());
                videoResponse.setVideoId(videos.get(i).getVideoId());
                videoResponses.add(videoResponse);
            }
            if (numVideos > 0) {
                submitAsyncIndexingTask(videos);
            }
        }
        return videoResponses;
    }

    private void submitAsyncIndexingTask(final List<Video> videos) {
        ExecutorService executer = Executors.newSingleThreadExecutor();
        final ChannelProcessor cp = this;
        Runnable task = new Runnable() {
            @Override
            public void run() {
                new ChannelVideoBulkInsertCommand(cp, videos).execute();
            }
        };
        executer.submit(task);
    }

    private List<Video> getVideosFromDM(@NotNull String channelId) throws DeException {
        if (StringUtils.isBlank(channelId)) {
            throw new DeException(new Throwable("No channel id provided"), HttpStatus.BAD_REQUEST_400);
        }
        try {
            HttpGet httpget = new HttpGet(dmUserVideoUrl.get().replace("{name}", channelId));

            logger.info("Executing request " + httpget.getRequestLine());

            // Create a custom response handler
            ResponseHandler<String> responseHandler = new ResponseHandler<String>() {

                public String handleResponse(
                        final HttpResponse response) throws ClientProtocolException, IOException {
                    int status = response.getStatusLine().getStatusCode();
                    if (HttpStatus.isSuccess(status)) {
                        HttpEntity entity = response.getEntity();
                        return entity != null ? EntityUtils.toString(entity) : null;
                    } else {
                        throw new ClientProtocolException("Unexpected response status: " + status);
                    }
                }

            };
            String responseBody = httpclient.execute(httpget, responseHandler);
            logger.info("----------------------------------------");
            logger.info(responseBody);
            ChannelVideos channelVideos = OBJECT_MAPPER.readValue(responseBody, ChannelVideos.class);
            return getFilteredVideos(channelVideos);

        } catch (ClientProtocolException e) {
            throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
        } catch (IOException e) {
            throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
        }
    }

    private List<Video> getFilteredVideos(ChannelVideos channelVideos) {
        List<Video> videos = new ArrayList<Video>();
        for (ChannelVideo channelVideo : channelVideos.getList()) {
            if (!filterVideo(channelVideo)) {

                Video video = new Video();
                video.setLanguages(Arrays.asList(channelVideo.getLanguage()));
                video.setThumbnailUrl(channelVideo.getThumbnailUrl());
                video.setTitle(channelVideo.getTitle());
                video.setDescription(channelVideo.getDescription());
                video.setDuration(channelVideo.getDuration());
                DateTimeFormatter df = DateTimeFormat.forPattern(DeHelper.getDateTimeFormatString());
                DateTime dt = new DateTime(channelVideo.getCreatedTime());
                video.setPublicationDate(dt.toString(df));
                video.setChannel(channelVideo.getOwnerUsername());
                video.setChannelId(channelVideo.getOwnerId());
                video.setCategories(Arrays.asList(channelVideo.getChannel()));
                video.setTags(channelVideo.getTags());
                video.setId(channelVideo.getVideoId());
                videos.add(video);
            }
        }
        return videos;
    }

    private Boolean filterVideo(ChannelVideo channelVideo) {
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
        if (!listOfValidCategories.get().contains(channelVideo.getChannel().toLowerCase())) {
            return true;
        }
        return false;
    }
}

