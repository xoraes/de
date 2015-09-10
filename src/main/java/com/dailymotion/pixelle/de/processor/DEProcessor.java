package com.dailymotion.pixelle.de.processor;

import com.dailymotion.pixelle.de.model.AdUnit;
import com.dailymotion.pixelle.de.model.AdUnitResponse;
import com.dailymotion.pixelle.de.model.ItemsResponse;
import com.dailymotion.pixelle.de.model.SearchQueryRequest;
import com.dailymotion.pixelle.de.model.Video;
import com.dailymotion.pixelle.de.model.VideoResponse;
import com.dailymotion.pixelle.de.processor.hystrix.AdQueryCommand;
import com.dailymotion.pixelle.de.processor.hystrix.ChannelQueryCommand;
import com.dailymotion.pixelle.de.processor.hystrix.VideoQueryCommand;
import com.google.inject.Inject;
import com.netflix.config.DynamicIntProperty;
import com.netflix.hystrix.exception.HystrixBadRequestException;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.BasicCounter;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.LongGauge;
import com.netflix.servo.monitor.StatsTimer;
import com.netflix.servo.monitor.Stopwatch;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.dailymotion.pixelle.de.processor.DeHelper.adunitsType;
import static com.dailymotion.pixelle.de.processor.DeHelper.currentUTCTime;
import static com.dailymotion.pixelle.de.processor.DeHelper.isEmptyList;
import static com.dailymotion.pixelle.de.processor.DeHelper.maxImpressions;
import static com.dailymotion.pixelle.de.processor.DeHelper.organicIndex;
import static com.dailymotion.pixelle.de.processor.DeHelper.promotedIndex;
import static com.dailymotion.pixelle.de.processor.DeHelper.timeToISO8601String;
import static com.dailymotion.pixelle.de.processor.DeHelper.videosType;
import static com.dailymotion.pixelle.de.processor.DeHelper.widgetPattern;
import static com.dailymotion.pixelle.de.processor.VideoProcessor.getUntargetedVideos;
import static com.netflix.config.DynamicPropertyFactory.getInstance;
import static com.netflix.servo.monitor.MonitorConfig.builder;
import static com.netflix.servo.stats.StatsConfig.Builder;
import static java.util.Arrays.asList;
import static java.util.Map.Entry;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang3.StringUtils.containsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.split;
import static org.eclipse.jetty.http.HttpStatus.BAD_REQUEST_400;
import static org.eclipse.jetty.http.HttpStatus.INTERNAL_SERVER_ERROR_500;
import static org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus.GREEN;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 12/10/14.
 */
public class DEProcessor {
    private static final Logger logger = getLogger(DEProcessor.class);
    private static final StatsTimer adsTimer = new StatsTimer(builder("adsQuery_statsTimer").build(), new Builder().withPublishMean(true).build());
    private static final StatsTimer videosTimer = new StatsTimer(builder("videosQuery_statsTimer").build(), new Builder().withPublishMean(true).build());
    private static final StatsTimer openWidgetTimer = new StatsTimer(builder("openWidgetQuery_statsTimer").build(), new Builder().withPublishMean(true).build());
    private static final StatsTimer channelWidgetTimer = new StatsTimer(builder("myWidgetTimerQuery_statsTimer").build(), new Builder().withPublishMean(true).build());
    private static final DynamicIntProperty MAX_CHANNELS = getInstance().getIntProperty("pixelle.channel.maxchannels", 7);
    private static final LongGauge videoCount = new LongGauge(builder("numVideos_gauge").build());
    private static final LongGauge adunitCount = new LongGauge(builder("numadUnits_gauge").build());
    private static final Counter maxImpressionThreshhold = new BasicCounter(
            builder("UserCrossedMaxImpressionThreshhold").build());
    private static Client client;

    static {
        DefaultMonitorRegistry.getInstance().register(adsTimer);
        DefaultMonitorRegistry.getInstance().register(videosTimer);
        DefaultMonitorRegistry.getInstance().register(openWidgetTimer);
        DefaultMonitorRegistry.getInstance().register(channelWidgetTimer);
        DefaultMonitorRegistry.getInstance().register(videoCount);
        DefaultMonitorRegistry.getInstance().register(adunitCount);
        DefaultMonitorRegistry.getInstance().register(maxImpressionThreshhold);
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
        String[] at = split(allowedTypes, ",", 2);
        sq = modifySearchQueryReq(sq);
        String pattern = sq.getPattern();
        if (StringUtils.isBlank(pattern) || !StringUtils.containsOnly(pattern,'p','o','P','O')) {
            pattern = widgetPattern.get();
        }

        if (at == null || at.length == 0) {
            stopwatch = adsTimer.start();
            try {
                ads = new AdQueryCommand(sq, positions).execute();
                itemsResponse.setResponse(ads);
            } finally {
                adsTimer.record(stopwatch.getDuration(MILLISECONDS), MILLISECONDS);
            }
            return itemsResponse;
        }

        if (at.length == 1 && containsIgnoreCase(at[0], "promoted")) {
            stopwatch = adsTimer.start();
            try {
                ads = new AdQueryCommand(sq, positions).execute();
                itemsResponse.setResponse(ads);
            } finally {
                adsTimer.record(stopwatch.getDuration(MILLISECONDS), MILLISECONDS);
            }
            return itemsResponse;
        } else if (at.length == 1 && containsIgnoreCase(at[0], "organic")) {
            stopwatch = videosTimer.start();
            try {
                targetedVideos = new VideoQueryCommand(sq, positions).execute();

                if (targetedVideos != null && targetedVideos.size() >= positions) {

                    itemsResponse.setResponse(targetedVideos);
                    return itemsResponse;
                } else {
                    List<VideoResponse> untargetedVideos = getUntargetedVideos(targetedVideos, positions, sq);
                    mergedList = mergeAndFillList(null, targetedVideos, untargetedVideos, positions, pattern);
                    itemsResponse.setResponse(mergedList);
                    return itemsResponse;
                }
            } finally {
                videosTimer.record(stopwatch.getDuration(MILLISECONDS), MILLISECONDS);
            }
        } else if (at.length == 2
                && containsIgnoreCase(allowedTypes, "promoted")
                && containsIgnoreCase(allowedTypes, "channel")) {
            if (isEmptyList(sq.getChannels()) && isBlank(sq.getChannel())) {
                throw new DeException(BAD_REQUEST_400, "No channels specified");
            }
            if (!isEmptyList(sq.getChannels()) && sq.getChannels().size() > MAX_CHANNELS.get()) {
                throw new DeException(BAD_REQUEST_400, "Too many channels specified");
            }
            stopwatch = channelWidgetTimer.start();
            try {
                Future<List<AdUnitResponse>> adsFuture = new AdQueryCommand(sq, positions).queue();
                Future<List<VideoResponse>> targetedVideosFuture = new ChannelQueryCommand(sq, positions).queue();

                try {
                    ads = adsFuture.get();
                    targetedVideos = targetedVideosFuture.get();
                } catch (InterruptedException e) {
                    throw new DeException(e, INTERNAL_SERVER_ERROR_500);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof HystrixBadRequestException) {
                        throw new DeException(e.getCause(), BAD_REQUEST_400);
                    }
                    logger.error("DE error while querying DM API");
                    throw new DeException(e, INTERNAL_SERVER_ERROR_500);
                }

                //if we have enough ads and videos, merge and send
                if (!isEmptyList(ads) && !isEmptyList(targetedVideos) && ads.size() + targetedVideos.size() >= positions) {
                    mergedList = mergeAndFillList(ads, targetedVideos, null, positions, pattern);
                    itemsResponse.setResponse(mergedList);
                    return itemsResponse;
                } else if (isEmptyList(ads) && targetedVideos.size() >= positions) { //ads empty, enough videos
                    itemsResponse.setResponse(targetedVideos);
                    return itemsResponse;
                } else { //fill with untargetged videos
                    List<VideoResponse> untargetedVideos = getUntargetedVideos(targetedVideos, positions, sq);
                    mergedList = mergeAndFillList(ads, targetedVideos, untargetedVideos, positions, pattern);
                    itemsResponse.setResponse(mergedList);
                    return itemsResponse;
                }
            } finally {
                channelWidgetTimer.record(stopwatch.getDuration(MILLISECONDS), MILLISECONDS);
            }
        } else if (at.length == 2
                && containsIgnoreCase(allowedTypes, "promoted")
                && containsIgnoreCase(allowedTypes, "organic")) {
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
                    throw new DeException(e, INTERNAL_SERVER_ERROR_500);
                } catch (ExecutionException e) {
                    throw new DeException(e, INTERNAL_SERVER_ERROR_500);
                }

                //if we have enough ads and videos, merge and send
                if (!isEmptyList(ads) && !isEmptyList(targetedVideos) && ads.size() + targetedVideos.size() >= positions) {
                    mergedList = mergeAndFillList(ads, targetedVideos, null, positions, pattern);
                    itemsResponse.setResponse(mergedList);
                    return itemsResponse;
                } else if (isEmptyList(ads) && targetedVideos.size() >= positions) { //ads empty, enough videos
                    itemsResponse.setResponse(targetedVideos);
                    return itemsResponse;
                } else { //fill with untargetged videos
                    List<VideoResponse> untargetedVideos = getUntargetedVideos(targetedVideos, positions, sq);
                    mergedList = mergeAndFillList(ads, targetedVideos, untargetedVideos, positions, pattern);
                    itemsResponse.setResponse(mergedList);
                    return itemsResponse;
                }
            } finally {
                openWidgetTimer.record(stopwatch.getDuration(MILLISECONDS), MILLISECONDS);
            }
        }
        return itemsResponse;
    }


    public static List<AdUnit> getAdUnitsByCampaign(String cid) throws DeException {
        return AdUnitProcessor.getAdUnitsByCampaign(cid);
    }


    public static AdUnit getAdUnitById(String id) throws DeException {
        return AdUnitProcessor.getAdUnitById(id);
    }


    public static Video getVideoById(String id) throws DeException {
        return VideoProcessor.getVideoById(id);
    }


    public static List<AdUnit> getAllAdUnits() throws DeException {
        return AdUnitProcessor.getAllAdUnits();
    }

    public static boolean deleteById(String indexName, String type, String id) throws DeException {
        if (isBlank(indexName) || isBlank(type) || isBlank(id)) {
            return false;
        }
        return client.prepareDelete(indexName, type, id).execute().actionGet().isFound();
    }

    /*
       assume pattern is not empty
     */
    private static List<? extends ItemsResponse> mergeAndFillList(final List<AdUnitResponse> ads,
                                                                  final List<VideoResponse> targetedVideos,
                                                                  final List<VideoResponse> untargetedVideos,
                                                                  final Integer positions,
                                                                  final String pattern) {

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

    public static void deleteIndex(String indexName) {
        if (client.admin().indices().prepareExists(indexName).execute().actionGet().isExists()
                && client.admin().indices().prepareDelete(indexName).execute().actionGet().isAcknowledged()) {
            logger.info("successfully deleted index: " + promotedIndex.get());
        }
    }

    private static SearchQueryRequest modifySearchQueryReq(SearchQueryRequest sq) {
        if (sq != null) {
            if (isEmptyList(sq.getCategories())) {
                sq.setCategories(asList("all"));
            } else {
                sq.getCategories().add("all");
            }
            if (isEmptyList(sq.getLocations())) {
                sq.setLocations(asList("all"));
            } else {
                sq.getLocations().add("all");
            }
            if (isEmptyList(sq.getLanguages())) {
                sq.setLanguages(asList("all"));
            } else {
                sq.getLanguages().add("all");
            }
            if (isBlank(sq.getTime())) {
                sq.setTime(timeToISO8601String(currentUTCTime()));
            }
            // add the adunit to excluded list if the user has already gotten a "lot"
            // of impressions. "Lot" is defined globally visible and defined value.
            // Incrment counter to track this globally
            Map<String, Integer> impressionHistory = sq.getImpressionHistory();
            if (impressionHistory != null && impressionHistory.size() != 0) {
                List<String> exList = new ArrayList<>();
                for (Entry<String, Integer> entry : impressionHistory.entrySet()) {
                    if (entry.getValue() >= maxImpressions.get()) {
                        exList.add(entry.getKey());
                        maxImpressionThreshhold.increment();
                    }
                }
                if (!isEmptyList(sq.getExcludedVideoIds())) {
                    sq.getExcludedVideoIds().addAll(exList);
                } else {
                    sq.setExcludedVideoIds(exList);
                }
            }
        }
        return sq;
    }

    public String getHealthCheck() throws DeException {
        boolean adIndexExists = client.admin()
                .indices()
                .prepareExists(promotedIndex.get())
                .execute()
                .actionGet()
                .isExists();
        boolean videoIndexExists = client.admin()
                .indices()
                .prepareExists(organicIndex.get())
                .execute()
                .actionGet()
                .isExists();
        boolean adUnitTypeExists = client.admin()
                .indices()
                .prepareTypesExists(promotedIndex.get())
                .setTypes(adunitsType.get())
                .execute()
                .actionGet()
                .isExists();
        boolean videoTypeExists = client.admin()
                .indices()
                .prepareTypesExists(organicIndex.get())
                .setTypes(videosType.get())
                .execute()
                .actionGet()
                .isExists();

        if (adIndexExists && videoIndexExists && adUnitTypeExists && videoTypeExists) {

            CountResponse countResponse;
            countResponse = client.prepareCount(organicIndex.get()).setTypes(videosType.get()).execute().actionGet();
            videoCount.set(countResponse.getCount());
            countResponse = client.prepareCount(promotedIndex.get()).setTypes(adunitsType.get()).execute().actionGet();
            adunitCount.set(countResponse.getCount());

            if (client.admin()
                    .cluster()
                    .prepareHealth(promotedIndex.get(), organicIndex.get())
                    .execute()
                    .actionGet().getStatus() == GREEN) {
                return "OK";
            } else {
                throw new DeException(INTERNAL_SERVER_ERROR_500, "internal error - cluster health");
            }
        } else {
            throw new DeException(new Throwable("index or type does not exist"), INTERNAL_SERVER_ERROR_500);
        }
    }
}
