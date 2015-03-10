package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.ChannelVideo;
import com.dailymotion.pixelle.deserver.model.ChannelVideos;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.model.Video;
import com.dailymotion.pixelle.deserver.model.VideoResponse;
import com.dailymotion.pixelle.deserver.processor.hystrix.ChannelVideoBulkInsertCommand;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicLongProperty;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by n.dhupia on 2/27/15.
 */
public class ChannelProcessor extends VideoProcessor {
    private static final Integer MAX_RANDOM = 100;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final DynamicStringProperty dmUserVideoUrl = DynamicPropertyFactory.getInstance().getStringProperty("dm.video.url", "");
    private static final DynamicStringProperty listOfValidCategories = DynamicPropertyFactory.getInstance().getStringProperty("pixelle.channel.categories", "");
    private static final DynamicLongProperty refreshAfterWriteMins = DynamicPropertyFactory.getInstance().getLongProperty("pixelle.channel.refresh.write.minutes", 4);
    private static final DynamicIntProperty lruSize = DynamicPropertyFactory.getInstance().getIntProperty("pixelle.channel.lru.size", 1000);

    private static Logger logger = LoggerFactory.getLogger(ChannelProcessor.class);
    private static CloseableHttpClient httpclient = HttpClients.createDefault();

    private LoadingCache<String, List<Video>> videosCache = CacheBuilder.newBuilder().maximumSize(lruSize.get()).refreshAfterWrite(refreshAfterWriteMins.get(), TimeUnit.MINUTES)
            .build(
                    new CacheLoader<String, List<Video>>() {
                        @Override
                        public List<Video> load(String key) throws DeException {
                            logger.info("Caching and indexing video..");
                            List<Video> videos = getVideosFromDM(key);
                            submitAsyncIndexingTask(videos);
                            return videos;
                        }
                        @Override
                        public ListenableFuture<List<Video>> reload(final String channelId, List<Video> oldValue) throws Exception {
                            logger.info("Reloading cache for key " + channelId);
                            ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
                            ListenableFuture<List<Video>> listenableFuture;
                            try {
                                listenableFuture = executor.submit(new Callable<List<Video>>() {
                                    @Override
                                    public List<Video> call() throws Exception {
                                        List<Video> videos = getVideosFromDM(channelId);
                                        submitAsyncIndexingTask(videos);
                                        return videos;
                                    }
                                });
                                return listenableFuture;
                            } finally {
                                executor.shutdown();
                            }
                        }
                    });


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
        /*
         * User request first hits ES. If num positions are found, then done. Otherwise, look in the videoCache.
         * If videoCache is empty, it is loaded with data from DM - this happens during cold start.
         * If videoCache has non-stale data, it is returned.
         * If videoCache has stale data (6 min since last write), then stale data is returned
         * and fresh data is replaced into the cache asynchronously from DM and indexed to ES.
         */
        if (DeHelper.isEmptyList(videoResponses) || videoResponses.size() < positions) {
            List<Video> videos = null;
            try {
                videos = videosCache.get(sq.getChannel());
                logger.info("getting videos from cache");
            } catch (ExecutionException e) {
                throw new DeException(e.getCause(),HttpStatus.INTERNAL_SERVER_ERROR_500);
            }
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
        }
        logger.info("Num video responses:" + videoResponses.size());
        return videoResponses;
    }

    private void submitAsyncIndexingTask(final List<Video> videos) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        final ChannelProcessor cp = this;
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    indexVideos(videos);
                }
            });
        } finally {
            executor.shutdown();
        }
    }

    private void indexVideos(final List<Video> videos) {
        final ChannelProcessor cp = this;
        new ChannelVideoBulkInsertCommand(cp, videos).execute();
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

    private static List<Video> getFilteredVideos(ChannelVideos channelVideos) {
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
        if (!listOfValidCategories.get().contains(channelVideo.getChannel().toLowerCase())) {
            return true;
        }
        return false;
    }
}

