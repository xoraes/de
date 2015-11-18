package com.dailymotion.pixelle.common.services;

import com.dailymotion.pixelle.de.model.SearchQueryRequest;
import com.dailymotion.pixelle.de.model.Video;
import com.dailymotion.pixelle.de.model.VideoGroupKey;
import com.dailymotion.pixelle.de.model.VideoResponse;
import com.dailymotion.pixelle.de.processor.DeException;
import com.dailymotion.pixelle.de.processor.DeHelper;
import com.dailymotion.pixelle.de.processor.hystrix.DMApiQueryCommand;
import com.dailymotion.pixelle.forecast.processor.ForecastException;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicLongProperty;
import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import static com.dailymotion.pixelle.common.services.BigQuery.getCountryCountTable;
import static com.dailymotion.pixelle.common.services.BigQuery.getCountryCountTableFromFile;
import static com.dailymotion.pixelle.de.processor.ChannelProcessor.getFilteredVideos;
import static com.dailymotion.pixelle.de.processor.DeHelper.CATEGORIESBYCOUNTRY;
import static com.dailymotion.pixelle.de.processor.DeHelper.DEVICESBYCOUNTRY;
import static com.dailymotion.pixelle.de.processor.DeHelper.EVENTSBYCOUNTRY;
import static com.dailymotion.pixelle.de.processor.DeHelper.FORMATSBYCOUNTRY;
import static com.dailymotion.pixelle.de.processor.DeHelper.LANGUAGEBYCOUNTRY;
import static com.dailymotion.pixelle.de.processor.VideoProcessor.recommend;
import static com.google.common.cache.CacheBuilder.newBuilder;
import static com.google.common.util.concurrent.MoreExecutors.getExitingExecutorService;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.netflix.config.DynamicPropertyFactory.getInstance;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.eclipse.jetty.http.HttpStatus.INTERNAL_SERVER_ERROR_500;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 3/17/15.
 */
public class CacheService {
    private static final Logger logger = getLogger(CacheService.class);

    private static final DynamicLongProperty chanRefreshAfterWriteMins = getInstance().getLongProperty("pixelle.channel.refresh.write.minutes", 4);
    private static final DynamicLongProperty countryCountRefreshAfterWriteMins = getInstance().getLongProperty("pixelle.bq.countrycounts.refresh.write.minutes", 1440); // 24 hours
    private static final DynamicLongProperty videoRefreshAfterWriteMins = getInstance().getLongProperty("pixelle.organic.refresh.write.minutes", 1);
    private static final DynamicIntProperty channelLruSize = getInstance().getIntProperty("pixelle.channel.lru.size", 1000);
    private static final DynamicIntProperty videoLruSize = getInstance().getIntProperty("pixelle.organic.lru.size", 1000);
    private static final DynamicIntProperty eventLruSize = getInstance().getIntProperty("pixelle.bq.eventcounts.lru.size", 10);
    private static final DynamicIntProperty maxVideosToCache = getInstance().getIntProperty("pixelle.organic.cache.max", 20);
    //exiting service makes sure the thread terminates 120 secs after jvm shutdown
    private static final ListeningExecutorService cacheMaintainer = listeningDecorator(getExitingExecutorService((ThreadPoolExecutor) newCachedThreadPool()));

    private static final LoadingCache<String, Table<String, String, Long>> perCountryCountCache = newBuilder()
            .recordStats()
            .maximumSize(eventLruSize.get()).refreshAfterWrite(countryCountRefreshAfterWriteMins.get(), MINUTES)
            .build(
                    new CacheLoader<String, Table<String, String, Long>>() {
                        @Override
                        public Table<String, String, Long> load(String target) throws ForecastException {
                            logger.info("Caching and indexing.." + target);
                            return getCountryCountTableFromFile(target);
                        }

                        @Override
                        public ListenableFuture<Table<String, String, Long>> reload(final String target, Table<String, String, Long> oldValue) throws DeException {
                            logger.info("Reloading cache for key " + target);
                            return cacheMaintainer.submit(() -> getCountryCountTable(target));
                        }
                    });

    private static final LoadingCache<VideoGroupKey, List<VideoResponse>> groupVideosCache = newBuilder()
            .recordStats()
            .maximumSize(channelLruSize.get()).refreshAfterWrite(chanRefreshAfterWriteMins.get(), MINUTES)
            .build(
                    new CacheLoader<VideoGroupKey, List<VideoResponse>>() {
                        @Override
                        public List<VideoResponse> load(VideoGroupKey key) throws DeException {
                            logger.info("Caching and indexing channel video..");
                            List<Video> cVideos = new DMApiQueryCommand(key.getChannels(), key.getPlaylist(), key
                                    .getSortOrder())
                                    .execute();
                            if (cVideos != null) {
                                return getFilteredVideos(cVideos);
                            }
                            return null;
                        }

                        @Override
                        public ListenableFuture<List<VideoResponse>> reload(final VideoGroupKey key, List<VideoResponse>
                                oldValue) throws DeException {
                            logger.info("Reloading cache for key " + key.getChannels());
                            ListenableFuture<List<VideoResponse>> listenableFuture = cacheMaintainer.submit(() -> {
                                List<Video> cVideos = new DMApiQueryCommand(key.getChannels(), key.getPlaylist(), key
                                        .getSortOrder())
                                        .execute();
                                if (cVideos != null) {
                                    return getFilteredVideos(cVideos);
                                }
                                return null;
                            });
                            return listenableFuture;

                        }
                    });

    private static final LoadingCache<SearchQueryRequest, List<VideoResponse>> organicVideosCache = newBuilder()
            .recordStats()
            .maximumSize(videoLruSize.get()).refreshAfterWrite(videoRefreshAfterWriteMins.get(), MINUTES)
            .build(
                    new CacheLoader<SearchQueryRequest, List<VideoResponse>>() {
                        @Override
                        public List<VideoResponse> load(SearchQueryRequest sq) throws Exception {
                            List<VideoResponse> vr = recommend(sq, maxVideosToCache.get());
                            logger.info("Caching organic videos..:", sq.toString());
                            return vr;
                        }

                        @Override
                        public ListenableFuture<List<VideoResponse>> reload(final SearchQueryRequest sq, List<VideoResponse> oldValue) throws Exception {
                            logger.info("Reloading cache for key " + sq);
                            ListeningExecutorService executor = listeningDecorator(newSingleThreadExecutor());
                            ListenableFuture<List<VideoResponse>> listenableFuture;
                            try {
                                listenableFuture = executor.submit(() -> recommend(sq, maxVideosToCache.get()));
                                return listenableFuture;
                            } finally {
                                executor.shutdown();
                            }
                        }
                    });


    public static final LoadingCache<VideoGroupKey, List<VideoResponse>> getGroupVideosCache() {
        return groupVideosCache;
    }

    public static final LoadingCache<SearchQueryRequest, List<VideoResponse>> getOrganicVideosCache() {
        return organicVideosCache;
    }


    public static final LoadingCache<String, Table<String, String, Long>> getPerCountryCountCache() {
        return perCountryCountCache;
    }

    public static Table<String, String, Long> getCountryEventCountCache() throws ForecastException {
        try {
            return perCountryCountCache.get(EVENTSBYCOUNTRY);
        } catch (ExecutionException e) {
            throw new ForecastException(e, INTERNAL_SERVER_ERROR_500);
        }
    }

    public static Table<String, String, Long> getCountryDeviceCountCache() throws ForecastException {
        try {
            return perCountryCountCache.get(DEVICESBYCOUNTRY);
        } catch (ExecutionException e) {
            throw new ForecastException(e, INTERNAL_SERVER_ERROR_500);
        }
    }

    public static Table<String, String, Long> getCountryFormatCountCache() throws ForecastException {
        try {
            return perCountryCountCache.get(FORMATSBYCOUNTRY);
        } catch (ExecutionException e) {
            throw new ForecastException(e, INTERNAL_SERVER_ERROR_500);
        }
    }

    public static Table<String, String, Long> getCountryCategoryCountCache() throws ForecastException {
        try {
            return perCountryCountCache.get(CATEGORIESBYCOUNTRY);
        } catch (ExecutionException e) {
            throw new ForecastException(e, INTERNAL_SERVER_ERROR_500);
        }
    }

    public static Table<String, String, Long> getCountryLangCountCache() throws ForecastException {
        try {
            return perCountryCountCache.get(LANGUAGEBYCOUNTRY);
        } catch (ExecutionException e) {
            throw new ForecastException(e, INTERNAL_SERVER_ERROR_500);
        }
    }

    public static List<VideoResponse> getVideos(SearchQueryRequest sq, String sortOrder)
            throws DeException {

        String channels = DeHelper.listToString(sq.getChannels());
        String playlist = sq.getPlaylist();

        List<VideoResponse> videoResponses = null;
        try {
            videoResponses = groupVideosCache.get(new VideoGroupKey(channels, playlist, sortOrder));
        } catch (ExecutionException e) {
            throw new DeException(e, INTERNAL_SERVER_ERROR_500);
        }
        return videoResponses;
    }
}
