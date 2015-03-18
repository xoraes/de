package com.dailymotion.pixelle.deserver.processor.service;

import com.dailymotion.pixelle.deserver.model.ChannelVideos;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.model.Video;
import com.dailymotion.pixelle.deserver.model.VideoResponse;
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

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by n.dhupia on 3/17/15.
 */
public class CacheService {
    private static final Logger logger = LoggerFactory.getLogger(CacheService.class);
    private static final DynamicLongProperty chanRefreshAfterWriteMins = DynamicPropertyFactory.getInstance().getLongProperty("pixelle.channel.refresh.write.minutes", 4);
    private static final DynamicLongProperty videoRefreshAfterWriteMins = DynamicPropertyFactory.getInstance().getLongProperty("pixelle.organic.refresh.write.minutes", 2);
    private static final DynamicIntProperty channelLruSize = DynamicPropertyFactory.getInstance().getIntProperty("pixelle.channel.lru.size", 1000);
    private static final DynamicIntProperty videoLruSize = DynamicPropertyFactory.getInstance().getIntProperty("pixelle.organic.lru.size", 1000);
    private static final DynamicIntProperty maxVideosToCache = DynamicPropertyFactory.getInstance().getIntProperty("pixelle.organic.cache.max", 20);
    //exiting service makes sure the thread terminates 120 secs after jvm shutdown
    private static final ListeningExecutorService cacheMaintainer = MoreExecutors.listeningDecorator(MoreExecutors.getExitingExecutorService((ThreadPoolExecutor) Executors.newCachedThreadPool()));

    private static final LoadingCache<String, List<Video>> channelVideosCache = CacheBuilder.newBuilder()
            .recordStats()
            .maximumSize(channelLruSize.get()).refreshAfterWrite(chanRefreshAfterWriteMins.get(), TimeUnit.MINUTES)
            .build(
                    new CacheLoader<String, List<Video>>() {
                        @Override
                        public List<Video> load(String key) throws DeException {
                            logger.info("Caching and indexing channel video..");
                            ChannelVideos cVideos = new DMApiQueryCommand(key).execute();
                            List<Video> videos = ChannelProcessor.getFilteredVideos(cVideos);
                            ChannelProcessor.submitAsyncIndexingTask(videos);
                            return videos;
                        }

                        @Override
                        public ListenableFuture<List<Video>> reload(final String channelId, List<Video> oldValue) throws Exception {
                            logger.info("Reloading cache for key " + channelId);
                            ListenableFuture<List<Video>> listenableFuture = cacheMaintainer.submit(() -> {
                                ChannelVideos cVideos = new DMApiQueryCommand(channelId).execute();
                                List<Video> videos = ChannelProcessor.getFilteredVideos(cVideos);
                                ChannelProcessor.submitAsyncIndexingTask(videos);
                                return videos;
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
                            List<VideoResponse> vr = VideoProcessor.recommend(sq, maxVideosToCache.get(), null);
                            logger.warn("Caching organic videos..:", sq.toString());
                            return vr;
                        }

                        @Override
                        public ListenableFuture<List<VideoResponse>> reload(final SearchQueryRequest sq, List<VideoResponse> oldValue) throws Exception {
                            logger.info("Reloading cache for key " + sq);
                            ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
                            ListenableFuture<List<VideoResponse>> listenableFuture;
                            try {
                                listenableFuture = executor.submit(() -> VideoProcessor.recommend(sq, maxVideosToCache.get(), null));
                                return listenableFuture;
                            } finally {
                                executor.shutdown();
                            }
                        }
                    });

    public static final LoadingCache<String, List<Video>> getChannelVideosCache() {
        return channelVideosCache;
    }

    public static final LoadingCache<SearchQueryRequest, List<VideoResponse>> getOrganicVideosCache() {
        return organicVideosCache;
    }
}