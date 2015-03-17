package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.AdUnit;
import com.dailymotion.pixelle.deserver.model.AdUnitResponse;
import com.dailymotion.pixelle.deserver.model.ItemsResponse;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.model.Video;
import com.dailymotion.pixelle.deserver.model.VideoResponse;
import com.dailymotion.pixelle.deserver.processor.hystrix.AdQueryCommand;
import com.dailymotion.pixelle.deserver.processor.hystrix.ChannelQueryCommand;
import com.dailymotion.pixelle.deserver.processor.hystrix.VideoQueryCommand;
import com.google.inject.Inject;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.BasicGauge;
import com.netflix.servo.monitor.Gauge;
import com.netflix.servo.monitor.LongGauge;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.StatsTimer;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.stats.StatsConfig;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.http.HttpStatus;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by n.dhupia on 12/10/14.
 */
public class DEProcessor {
    private static final Logger logger = LoggerFactory.getLogger(DEProcessor.class);
    private static Client client;
    private static final StatsTimer adsTimer = new StatsTimer(MonitorConfig.builder("adsQuery_statsTimer").build(), new StatsConfig.Builder().withPublishMean(true).build());
    private static final StatsTimer videosTimer = new StatsTimer(MonitorConfig.builder("videosQuery_statsTimer").build(), new StatsConfig.Builder().withPublishMean(true).build());
    private static final StatsTimer openWidgetTimer = new StatsTimer(MonitorConfig.builder("openWidgetQuery_statsTimer").build(), new StatsConfig.Builder().withPublishMean(true).build());
    private static final StatsTimer channelWidgetTimer = new StatsTimer(MonitorConfig.builder("myWidgetTimerQuery_statsTimer").build(), new StatsConfig.Builder().withPublishMean(true).build());
    private static final LongGauge videoCount = new LongGauge(MonitorConfig.builder("numVideos_gauge").build());
    private static final LongGauge adunitCount = new LongGauge(MonitorConfig.builder("numadUnits_gauge").build());

    static {
        DefaultMonitorRegistry.getInstance().register(adsTimer);
        DefaultMonitorRegistry.getInstance().register(videosTimer);
        DefaultMonitorRegistry.getInstance().register(openWidgetTimer);
        DefaultMonitorRegistry.getInstance().register(channelWidgetTimer);
        DefaultMonitorRegistry.getInstance().register(videoCount);
        DefaultMonitorRegistry.getInstance().register(adunitCount);
    }

    @Inject
    public DEProcessor(Client esClient) {
        client = esClient;
    }


    public static ItemsResponse recommend(SearchQueryRequest sq, Integer positions, String allowedTypes) throws DeException {
        List<VideoResponse> targetedVideos;
        List<AdUnitResponse> ads;
        List<? extends ItemsResponse> mergedList;
        Stopwatch stopwatch;

        ItemsResponse itemsResponse = new ItemsResponse();
        String[] at = StringUtils.split(allowedTypes, ",", 2);
        sq = modifySearchQueryReq(sq);

        if (at == null || at.length == 0) {
            stopwatch = adsTimer.start();
            try {
                ads = new AdQueryCommand(sq, positions).execute();
                itemsResponse.setResponse(ads);
            } finally {
                adsTimer.record(stopwatch.getDuration(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
            }
            return itemsResponse;
        }

        if (at.length == 1 && StringUtils.containsIgnoreCase(at[0], "promoted")) {
            stopwatch = adsTimer.start();
            try {
                ads = new AdQueryCommand(sq, positions).execute();
                itemsResponse.setResponse(ads);
            } finally {
                adsTimer.record(stopwatch.getDuration(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
            }
            return itemsResponse;
        }

        if (at.length == 1 && StringUtils.containsIgnoreCase(at[0], "organic")) {
            stopwatch = videosTimer.start();
            try {
                targetedVideos = new VideoQueryCommand(sq, positions).execute();

                if (targetedVideos != null && targetedVideos.size() >= positions) {

                    itemsResponse.setResponse(targetedVideos);
                    return itemsResponse;
                } else {
                    List<VideoResponse> untargetedVideos = VideoProcessor.getUntargetedVideos(targetedVideos, positions, sq);
                    mergedList = mergeAndFillList(null, targetedVideos, untargetedVideos, positions);
                    itemsResponse.setResponse(mergedList);
                    return itemsResponse;
                }
            } finally {
                videosTimer.record(stopwatch.getDuration(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
            }
        }
        if (at.length == 2
                && StringUtils.containsIgnoreCase(allowedTypes, "promoted")
                && StringUtils.containsIgnoreCase(allowedTypes, "channel")) {
            stopwatch = channelWidgetTimer.start();
            try {
                Future<List<AdUnitResponse>> adsFuture;
                Future<List<VideoResponse>> targetedVideosFuture;

                adsFuture = new AdQueryCommand(sq, positions).queue();
                targetedVideosFuture = new ChannelQueryCommand(sq, positions).queue();

                try {
                    ads = adsFuture.get();
                    targetedVideos = targetedVideosFuture.get();
                } catch (InterruptedException e) {
                    throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
                } catch (ExecutionException e) {
                    throw new DeException(e.getCause(), HttpStatus.INTERNAL_SERVER_ERROR_500);
                }

                //if we have enough ads and videos, merge and send
                if (!DeHelper.isEmptyList(ads) && !DeHelper.isEmptyList(targetedVideos) && ads.size() + targetedVideos.size() >= positions) {
                    mergedList = mergeAndFillList(ads, targetedVideos, null, positions);
                    itemsResponse.setResponse(mergedList);
                    return itemsResponse;
                } else if (DeHelper.isEmptyList(ads) && targetedVideos.size() >= positions) { //ads empty, enough videos
                    itemsResponse.setResponse(targetedVideos);
                    return itemsResponse;
                } else { //fill with untargetged videos
                    List<VideoResponse> untargetedVideos = VideoProcessor.getUntargetedVideos(targetedVideos, positions, sq);
                    mergedList = mergeAndFillList(ads, targetedVideos, untargetedVideos, positions);
                    itemsResponse.setResponse(mergedList);
                    return itemsResponse;
                }
            } finally {
                channelWidgetTimer.record(stopwatch.getDuration(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
            }
        }

        if (at.length == 2
                && StringUtils.containsIgnoreCase(allowedTypes, "promoted")
                && StringUtils.containsIgnoreCase(allowedTypes, "organic")) {
            stopwatch = openWidgetTimer.start();
            try {
                Future<List<AdUnitResponse>> adsFuture;
                Future<List<VideoResponse>> targetedVideosFuture;

                adsFuture = new AdQueryCommand(sq, positions).queue();
                targetedVideosFuture = new VideoQueryCommand(sq, positions).queue();

                try {
                    ads = adsFuture.get();
                    targetedVideos = targetedVideosFuture.get();
                } catch (InterruptedException e) {
                    throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
                } catch (ExecutionException e) {
                    throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
                }

                //if we have enough ads and videos, merge and send
                if (!DeHelper.isEmptyList(ads) && !DeHelper.isEmptyList(targetedVideos) && ads.size() + targetedVideos.size() >= positions) {
                    mergedList = mergeAndFillList(ads, targetedVideos, null, positions);
                    itemsResponse.setResponse(mergedList);
                    return itemsResponse;
                } else if (DeHelper.isEmptyList(ads) && targetedVideos.size() >= positions) { //ads empty, enough videos
                    itemsResponse.setResponse(targetedVideos);
                    return itemsResponse;
                } else { //fill with untargetged videos
                    List<VideoResponse> untargetedVideos = VideoProcessor.getUntargetedVideos(targetedVideos, positions, sq);
                    mergedList = mergeAndFillList(ads, targetedVideos, untargetedVideos, positions);
                    itemsResponse.setResponse(mergedList);
                    return itemsResponse;
                }
            } finally {
                openWidgetTimer.record(stopwatch.getDuration(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
            }
        }
        return itemsResponse;
    }


    public static List<AdUnit> getAdUnitsByCampaign(String cid) {
        return AdUnitProcessor.getAdUnitsByCampaign(cid);
    }


    public static AdUnit getAdUnitById(String id) {
        return AdUnitProcessor.getAdUnitById(id);
    }


    public static Video getVideoById(String id) throws DeException {
        return VideoProcessor.getVideoById(id);
    }


    public static List<AdUnit> getAllAdUnits() {
        return AdUnitProcessor.getAllAdUnits();
    }

    public static void updateVideo(Video video) throws DeException {
        VideoProcessor.updateVideo(video);
    }

    public static boolean deleteById(String indexName, String type, String id) throws DeException {
        if (StringUtils.isBlank(indexName) || StringUtils.isBlank(type) || StringUtils.isBlank(id)) {
            return false;
        }
        return client.prepareDelete(indexName, type, id).execute().actionGet().isFound();
    }

    private static List<? extends ItemsResponse> mergeAndFillList(final List<AdUnitResponse> ads,
                                                                  final List<VideoResponse> targetedVideos,
                                                                  final List<VideoResponse> untargetedVideos,
                                                                  final Integer positions) {

        String pattern = DeHelper.widgetPattern.get();
        int len = pattern.length();

        List<ItemsResponse> items = new ArrayList<ItemsResponse>();
        int adIter = 0, videoIter = 0, untargetedVideoIter = 0, patternIter = 0;

        for (int positionsFilled = 0; positionsFilled < positions; patternIter++, positionsFilled++) {
            if (ads != null && ads.size() > adIter && (pattern.charAt(patternIter % len) == 'P' || pattern.charAt(patternIter % len) == 'p')) {
                items.add(ads.get(adIter++));
            } else if (targetedVideos != null && targetedVideos.size() > videoIter && (pattern.charAt(patternIter % len) == 'O' || pattern.charAt(patternIter % len) == 'o')) {
                items.add(targetedVideos.get(videoIter++));
            } else if (targetedVideos != null && targetedVideos.size() > videoIter) { // not enough ads, so fill with all available targeted videos
                items.add(targetedVideos.get(videoIter++));
            } else if (untargetedVideos != null && untargetedVideos.size() > untargetedVideoIter) { //not enough targeted videos, so fill it with un-targeted videos
                items.add(untargetedVideos.get(untargetedVideoIter++));
            } else if (ads != null && ads.size() > adIter) { //not enough targeted videos, so fill with targeted ads - rare case
                items.add(ads.get(adIter++));
            } else { // don't bother filling with un-targeted ads
                break;
            }
        }
        return items;
    }

    public static void insertVideoInBulk(List<Video> videos) throws DeException {
        VideoProcessor.insertVideoInBulk(videos);
    }

    static void deleteIndex(String indexName) {
        if (client.admin().indices().prepareExists(indexName).execute().actionGet().isExists()
                && client.admin().indices().prepareDelete(indexName).execute().actionGet().isAcknowledged()) {
            logger.info("successfully deleted index: " + DeHelper.promotedIndex.get());
        }
    }

    private static SearchQueryRequest modifySearchQueryReq(SearchQueryRequest sq) {
        if (sq != null) {
            if (DeHelper.isEmptyList(sq.getCategories())) {
                sq.setCategories(Arrays.asList("all"));
            } else {
                sq.getCategories().add("all");
            }
            if (DeHelper.isEmptyList(sq.getLocations())) {
                sq.setLocations(Arrays.asList("all"));
            } else {
                sq.getLocations().add("all");
            }
            if (DeHelper.isEmptyList(sq.getLanguages())) {
                sq.setLanguages(Arrays.asList("all"));
            } else {
                sq.getLanguages().add("all");
            }
            if (StringUtils.isBlank(sq.getTime())) {
                sq.setTime(DeHelper.timeToISO8601String(DeHelper.currentUTCTime()));
            }
        }
        return sq;
    }

    public void insertVideo(Video video) throws DeException {
        VideoProcessor.insertVideo(video);
    }

    public ClusterHealthResponse getHealthCheck() throws DeException {
        boolean adIndexExists = client.admin()
                .indices()
                .prepareExists(DeHelper.promotedIndex.get())
                .execute()
                .actionGet()
                .isExists();
        boolean videoIndexExists = client.admin()
                .indices()
                .prepareExists(DeHelper.organicIndex.get())
                .execute()
                .actionGet()
                .isExists();
        boolean adUnitTypeExists = client.admin()
                .indices()
                .prepareTypesExists(DeHelper.promotedIndex.get())
                .setTypes(DeHelper.adunitsType.get())
                .execute()
                .actionGet()
                .isExists();
        boolean videoTypeExists = client.admin()
                .indices()
                .prepareTypesExists(DeHelper.organicIndex.get())
                .setTypes(DeHelper.videosType.get())
                .execute()
                .actionGet()
                .isExists();

        if (adIndexExists && videoIndexExists && adUnitTypeExists && videoTypeExists) {

            CountResponse countResponse;
            countResponse = client.prepareCount(DeHelper.promotedIndex.get()).setTypes(DeHelper.adunitsType.get()).execute().actionGet();
            videoCount.set(countResponse.getCount());
            countResponse = client.prepareCount(DeHelper.promotedIndex.get()).setTypes(DeHelper.adunitsType.get()).execute().actionGet();
            adunitCount.set(countResponse.getCount());

            return client.admin()
                    .cluster()
                    .prepareHealth(DeHelper.promotedIndex.get(), DeHelper.organicIndex.get())
                    .execute()
                    .actionGet();
        } else {
            throw new DeException(new Throwable("index or type does not exist"), HttpStatus.INTERNAL_SERVER_ERROR_500);
        }
    }
}
