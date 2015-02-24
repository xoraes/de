package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.*;
import com.dailymotion.pixelle.deserver.processor.hystrix.AdQueryCommand;
import com.dailymotion.pixelle.deserver.processor.hystrix.VideoQueryCommand;
import com.google.inject.Inject;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.StatsTimer;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.stats.StatsConfig;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.http.HttpStatus;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by n.dhupia on 12/10/14.
 */
public class DEProcessor {
    private static final String ZEROTIME = "0001-01-01T00:00:00Z";
    private static Logger logger = LoggerFactory.getLogger(DEProcessor.class);
    private static Client client;
    private static AdUnitProcessor adUnitProcessor;
    private static VideoProcessor videoProcessor;
    private static StatsTimer adsTimer = new StatsTimer(MonitorConfig.builder("adsQuery_statsTimer").build(), new StatsConfig.Builder().withPublishMean(true).build());
    private static StatsTimer videosTimer = new StatsTimer(MonitorConfig.builder("videosQuery_statsTimer").build(), new StatsConfig.Builder().withPublishMean(true).build());
    private static StatsTimer widgetTimer = new StatsTimer(MonitorConfig.builder("widgetQuery_statsTimer").build(), new StatsConfig.Builder().withPublishMean(true).build());

    static {
        DefaultMonitorRegistry.getInstance().register(adsTimer);
        DefaultMonitorRegistry.getInstance().register(videosTimer);
        DefaultMonitorRegistry.getInstance().register(widgetTimer);
    }

    @Inject
    public DEProcessor(Client esClient, AdUnitProcessor adUnitProcessor, VideoProcessor videoProcessor) {
        client = esClient;
        DEProcessor.adUnitProcessor = adUnitProcessor;
        DEProcessor.videoProcessor = videoProcessor;
    }


    public ItemsResponse recommend(SearchQueryRequest sq, Integer positions, String allowedTypes) throws DeException {
        List<VideoResponse> targetedVideos = null;
        List<AdUnitResponse> ads = null;
        List<? extends ItemsResponse> mergedList = null;
        Stopwatch stopwatch;

        ItemsResponse itemsResponse = new ItemsResponse();
        String[] at = StringUtils.split(allowedTypes, ",", 2);
        sq = DeHelper.modifySearchQueryReq(sq);

        if (at == null || at.length == 0) {
            stopwatch = adsTimer.start();
            try {
                ads = new AdQueryCommand(adUnitProcessor, sq, positions).execute();
                itemsResponse.setResponse(ads);
            } finally {
                adsTimer.record(stopwatch.getDuration(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
            }
            return itemsResponse;
        }

        if (at.length == 1 && StringUtils.containsIgnoreCase(at[0], "promoted")) {
            stopwatch = adsTimer.start();
            try {
                ads = new AdQueryCommand(adUnitProcessor, sq, positions).execute();
                itemsResponse.setResponse(ads);
            } finally {
                adsTimer.record(stopwatch.getDuration(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
            }
            return itemsResponse;
        }

        if (at.length == 1 && StringUtils.containsIgnoreCase(at[0], "organic")) {
            stopwatch = videosTimer.start();
            try {
                targetedVideos = new VideoQueryCommand(videoProcessor, sq, positions).execute();

                if (targetedVideos != null && targetedVideos.size() >= positions) {

                    itemsResponse.setResponse(targetedVideos);
                    return itemsResponse;
                } else {
                    List<VideoResponse> untargetedVideos = videoProcessor.getUntargetedVideos(targetedVideos, positions, sq);
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
                && StringUtils.containsIgnoreCase(allowedTypes, "organic")) {
            stopwatch = widgetTimer.start();
            try {
                Future<List<AdUnitResponse>> adsFuture;
                Future<List<VideoResponse>> targetedVideosFuture;

                adsFuture = new AdQueryCommand(adUnitProcessor, sq, positions).queue();
                targetedVideosFuture = new VideoQueryCommand(videoProcessor, sq, positions).queue();

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
                    List<VideoResponse> untargetedVideos = videoProcessor.getUntargetedVideos(targetedVideos, positions, sq);
                    mergedList = mergeAndFillList(ads, targetedVideos, untargetedVideos, positions);
                    itemsResponse.setResponse(mergedList);
                    return itemsResponse;
                }
            } finally {
                widgetTimer.record(stopwatch.getDuration(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
            }
        }
        return itemsResponse;
    }


    public List<AdUnit> getAdUnitsByCampaign(String cid) {
        return adUnitProcessor.getAdUnitsByCampaign(cid);
    }


    public void insertAdUnit(AdUnit unit) throws DeException {
        adUnitProcessor.insertAdUnit(unit);
    }


    public void updateAdUnit(AdUnit unit) throws DeException {
        adUnitProcessor.updateAdUnit(unit);
    }


    public AdUnit getAdUnitById(String id) {
        return adUnitProcessor.getAdUnitById(id);
    }


    public Video getVideoById(String id) throws DeException {
        return videoProcessor.getVideoById(id);
    }


    public List<AdUnit> getAllAdUnits() {
        return adUnitProcessor.getAllAdUnits();
    }


    public void insertVideo(Video video) throws DeException {
        videoProcessor.insertVideo(video);
    }


    public void updateVideo(Video video) throws DeException {
        videoProcessor.updateVideo(video);
    }

    public ClusterHealthResponse getHealthCheck() throws DeException {
        boolean adIndexExists = client.admin()
                .indices()
                .prepareExists(DeHelper.getPromotedIndex())
                .execute()
                .actionGet()
                .isExists();
        boolean videoIndexExists = client.admin()
                .indices()
                .prepareExists(DeHelper.getOrganicIndex())
                .execute()
                .actionGet()
                .isExists();
        boolean adUnitTypeExists = client.admin()
                .indices()
                .prepareTypesExists(DeHelper.getPromotedIndex())
                .setTypes(DeHelper.getAdUnitsType())
                .execute()
                .actionGet()
                .isExists();
        boolean videoTypeExists = client.admin()
                .indices()
                .prepareTypesExists(DeHelper.getOrganicIndex())
                .setTypes(DeHelper.getVideosType())
                .execute()
                .actionGet()
                .isExists();

        if (adIndexExists && videoIndexExists && adUnitTypeExists && videoTypeExists) {
            return client.admin()
                    .cluster()
                    .prepareHealth(DeHelper.getPromotedIndex(), DeHelper.getOrganicIndex())
                    .execute()
                    .actionGet();
        } else {
            throw new DeException(new Throwable("index or type does not exist"), HttpStatus.INTERNAL_SERVER_ERROR_500);
        }
    }


    public boolean deleteById(String indexName, String type, String id) throws DeException {
        if (StringUtils.isBlank(indexName) || StringUtils.isBlank(type) || StringUtils.isBlank(id)) {
            return false;
        }
        return client.prepareDelete(indexName, type, id).execute().actionGet().isFound();
    }

    public List<? extends ItemsResponse> mergeAndFillList(final List<AdUnitResponse> ads, final List<VideoResponse> targetedVideos, final List<VideoResponse> untargetedVideos, final Integer positions) {

        String pattern = DeHelper.getWidgetPattern();
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

    public void insertAdUnitsInBulk(List<AdUnit> adUnits) throws DeException {
        adUnitProcessor.insertAdUnitsInBulk(adUnits);
    }

    public void insertVideoInBulk(List<Video> videos) throws DeException {
        videoProcessor.insertVideoInBulk(videos);
    }

    protected void deleteIndex(String indexName) {
        if (client.admin().indices().prepareExists(indexName).execute().actionGet().isExists()
                && client.admin().indices().prepareDelete(indexName).execute().actionGet().isAcknowledged()) {
            logger.info("successfully deleted index: " + DeHelper.getPromotedIndex());
        }
    }
}
