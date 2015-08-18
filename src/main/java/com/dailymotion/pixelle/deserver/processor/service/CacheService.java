package com.dailymotion.pixelle.deserver.processor.service;

import com.dailymotion.pixelle.deserver.model.*;
import com.dailymotion.pixelle.deserver.processor.ChannelProcessor;
import com.dailymotion.pixelle.deserver.processor.DeException;
import com.dailymotion.pixelle.deserver.processor.VideoProcessor;
import com.dailymotion.pixelle.deserver.processor.hystrix.DMApiQueryCommand;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicLongProperty;
import com.netflix.config.DynamicPropertyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by n.dhupia on 3/17/15.
 */
public class CacheService {
    private static final Logger logger = LoggerFactory.getLogger(CacheService.class);
    private static final DynamicLongProperty chanRefreshAfterWriteMins = DynamicPropertyFactory.getInstance().getLongProperty("pixelle.channel.refresh.write.minutes", 4);
    private static final DynamicLongProperty eventCountRefreshAfterWriteMins = DynamicPropertyFactory.getInstance().getLongProperty("pixelle.bq.eventcounts.refresh.write.minutes", 1440); // 24 hours
    private static final DynamicLongProperty videoRefreshAfterWriteMins = DynamicPropertyFactory.getInstance().getLongProperty("pixelle.organic.refresh.write.minutes", 1);
    private static final DynamicIntProperty channelLruSize = DynamicPropertyFactory.getInstance().getIntProperty("pixelle.channel.lru.size", 1000);
    private static final DynamicIntProperty videoLruSize = DynamicPropertyFactory.getInstance().getIntProperty("pixelle.organic.lru.size", 1000);
    private static final DynamicIntProperty eventLruSize = DynamicPropertyFactory.getInstance().getIntProperty("pixelle.bq.eventcounts.lru.size", 10);
    private static final DynamicIntProperty maxVideosToCache = DynamicPropertyFactory.getInstance().getIntProperty("pixelle.organic.cache.max", 20);
    //exiting service makes sure the thread terminates 120 secs after jvm shutdown
    private static final ListeningExecutorService cacheMaintainer = MoreExecutors.listeningDecorator(MoreExecutors.getExitingExecutorService((ThreadPoolExecutor) Executors.newCachedThreadPool()));

    private static final LoadingCache<String, Map<String, Long>> eventCountCache = CacheBuilder.newBuilder()
            .recordStats()
            .maximumSize(eventLruSize.get()).refreshAfterWrite(eventCountRefreshAfterWriteMins.get(), TimeUnit.MINUTES)
            .build(
                    new CacheLoader<String, Map<String, Long>>() {
                        @Override
                        public Map<String, Long> load(String event) throws DeException {
                            logger.info("Caching and indexing channel video..");
                            try {
                                return BigQuery.getEventCountMap(event);
                            } catch (IOException e) {
                                throw new DeException(e, 500);
                            } catch (InterruptedException e) {
                                throw new DeException(e, 500);
                            } catch (GeneralSecurityException e) {
                                throw new DeException(e, 500);
                            }
                        }

                        @Override
                        public ListenableFuture<Map<String, Long>> reload(final String event, Map<String, Long> oldValue) throws DeException {
                            logger.info("Reloading cache for key " + event);
                            return cacheMaintainer.submit(() -> BigQuery.getEventCountMap(event));
                        }
                    });

    private static final LoadingCache<Channels, List<Video>> channelVideosCache = CacheBuilder.newBuilder()
            .recordStats()
            .maximumSize(channelLruSize.get()).refreshAfterWrite(chanRefreshAfterWriteMins.get(), TimeUnit.MINUTES)
            .build(
                    new CacheLoader<Channels, List<Video>>() {
                        @Override
                        public List<Video> load(Channels channels) throws DeException {
                            logger.info("Caching and indexing channel video..");
                            ChannelVideos cVideos = new DMApiQueryCommand(channels).execute();
                            if (cVideos != null) {
                                return ChannelProcessor.getFilteredVideos(cVideos);
                            }
                            return null;
                        }

                        @Override
                        public ListenableFuture<List<Video>> reload(final Channels channels, List<Video> oldValue) throws DeException {
                            logger.info("Reloading cache for key " + channels.getChannels());
                            ListenableFuture<List<Video>> listenableFuture = cacheMaintainer.submit(() -> {
                                ChannelVideos cVideos = new DMApiQueryCommand(channels).execute();
                                if (cVideos != null) {
                                    return ChannelProcessor.getFilteredVideos(cVideos);
                                }
                                return null;
                            });
                            return listenableFuture;

                        }
                    });

    private static final LoadingCache<SearchQueryRequest, List<VideoResponse>> organicVideosCache = CacheBuilder.newBuilder()
            .recordStats()
            .maximumSize(videoLruSize.get()).refreshAfterWrite(videoRefreshAfterWriteMins.get(), TimeUnit.MINUTES)
            .build(
                    new CacheLoader<SearchQueryRequest, List<VideoResponse>>() {
                        @Override
                        public List<VideoResponse> load(SearchQueryRequest sq) throws Exception {
                            List<VideoResponse> vr = VideoProcessor.recommend(sq, maxVideosToCache.get());
                            logger.info("Caching organic videos..:", sq.toString());
                            return vr;
                        }

                        @Override
                        public ListenableFuture<List<VideoResponse>> reload(final SearchQueryRequest sq, List<VideoResponse> oldValue) throws Exception {
                            logger.info("Reloading cache for key " + sq);
                            ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
                            ListenableFuture<List<VideoResponse>> listenableFuture;
                            try {
                                listenableFuture = executor.submit(() -> VideoProcessor.recommend(sq, maxVideosToCache.get()));
                                return listenableFuture;
                            } finally {
                                executor.shutdown();
                            }
                        }
                    });

    public static final LoadingCache<Channels, List<Video>> getChannelVideosCache() {
        return channelVideosCache;
    }

    public static final LoadingCache<SearchQueryRequest, List<VideoResponse>> getOrganicVideosCache() {
        return organicVideosCache;
    }

    public static final LoadingCache<String, Map<String, Long>> getEventCountCache() {
        return eventCountCache;
    }
}
