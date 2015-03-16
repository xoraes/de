package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.ChannelVideo;
import com.dailymotion.pixelle.deserver.model.ChannelVideos;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.model.Video;
import com.dailymotion.pixelle.deserver.model.VideoResponse;
import com.dailymotion.pixelle.deserver.processor.hystrix.ChannelVideoBulkInsertCommand;
import com.dailymotion.pixelle.deserver.processor.hystrix.DMApiQueryCommand;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicLongProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import feign.Feign;
import feign.Retryer;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
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

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by n.dhupia on 2/27/15.
 */
public class ChannelProcessor extends VideoProcessor {
    private static final Integer MAX_RANDOM = 100;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final DynamicStringProperty dmApiUrl = DynamicPropertyFactory.getInstance().getStringProperty("dm.api.url", "https://api.dailymotion.com");
    private static final DynamicLongProperty retryPeriod = DynamicPropertyFactory.getInstance().getLongProperty("dm.api.retry.period", 100);
    private static final DynamicLongProperty retryMaxPeriod = DynamicPropertyFactory.getInstance().getLongProperty("dm.api.retry.max.period", 1);
    private static final DynamicIntProperty retryMaxAttempts = DynamicPropertyFactory.getInstance().getIntProperty("dm.api.retry.max.attempts", 5);

    private static final DynamicStringProperty listOfValidCategories = DynamicPropertyFactory.getInstance().getStringProperty("pixelle.channel.categories", "");
    private static final DynamicLongProperty refreshAfterWriteMins = DynamicPropertyFactory.getInstance().getLongProperty("pixelle.channel.refresh.write.minutes", 4);
    private static final DynamicIntProperty lruSize = DynamicPropertyFactory.getInstance().getIntProperty("pixelle.channel.lru.size", 1000);
    private static final DynamicBooleanProperty persistChanneltoES = DynamicPropertyFactory.getInstance().getBooleanProperty("pixelle.channel.es.persist", false);


    private static final Logger logger = LoggerFactory.getLogger(ChannelProcessor.class);
    private static CloseableHttpClient httpclient = HttpClients.createDefault();

    private static final LoadingCache<String, List<Video>> videosCache = CacheBuilder.newBuilder().maximumSize(lruSize.get()).refreshAfterWrite(refreshAfterWriteMins.get(), TimeUnit.MINUTES)
            .build(
                    new CacheLoader<String, List<Video>>() {
                        @Override
                        public List<Video> load(String key) throws DeException {
                            logger.info("Caching and indexing video..");
                            List<Video> videos = new DMApiQueryCommand(key).execute();
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
                                        List<Video> videos = new DMApiQueryCommand(channelId).execute();
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

    public static List<VideoResponse> recommend(SearchQueryRequest sq, Integer positions) throws DeException {

        List<VideoResponse> videoResponses;
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
                VideoResponse videoResponse = OBJECT_MAPPER.readValue(hit.getSourceAsString(), VideoResponse.class);
                videoResponses.add(videoResponse);
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
            videoResponses = new ArrayList<VideoResponse>();
            List<Video> videos;
            try {
                videos = videosCache.get(sq.getChannel());
                logger.info("getting videos from cache");
            } catch (ExecutionException e) {
                throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
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
                videoResponse.setResizableThumbnailUrl(videos.get(i).getResizableThumbnailUrl());
                videoResponse.setDuration(videos.get(i).getDuration());
                videoResponse.setVideoId(videos.get(i).getVideoId());
                videoResponses.add(videoResponse);
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
        String id = thumbnailUrl.substring(thumbnailUrl.lastIndexOf("/") + 1, thumbnailUrl.lastIndexOf("."));
        if (StringUtils.isBlank(id)) {
            return null;
        }
        return "//i.".concat(DeHelper.domain.get()).concat("/channel-").concat(id).concat("-thumbnail-1");
    }

    private static void submitAsyncIndexingTask(final List<Video> videos) {
        if (!DeHelper.isEmptyList(videos) && persistChanneltoES.get()) {
            new ChannelVideoBulkInsertCommand(videos).queue();
        }
    }

    public static List<Video> getVideosFromDM(@NotNull String channelId) throws DeException {
        if (StringUtils.isBlank(channelId)) {
            throw new DeException(new Throwable("No channel id provided"), HttpStatus.BAD_REQUEST_400);
        }
        DMApiService dmApi = Feign.builder()
                .retryer(new Retryer.Default(retryPeriod.get(), TimeUnit.SECONDS.toMillis(retryMaxPeriod.get()), retryMaxAttempts.get()))
                .decoder(new JacksonDecoder())
                .encoder(new JacksonEncoder())
                .target(DMApiService.class, dmApiUrl.get());
        return getFilteredVideos(dmApi.getVideos(channelId));
    }

    private static List<Video> getFilteredVideos(ChannelVideos channelVideos) {
        List<Video> videos = new ArrayList<Video>();
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

