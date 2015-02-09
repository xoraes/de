package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.*;
import com.dailymotion.pixelle.deserver.processor.hystrix.AdQueryCommand;
import com.dailymotion.pixelle.deserver.processor.hystrix.VideoQueryCommand;
import com.dailymotion.pixelle.deserver.providers.ESIndexTypeFactory;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by n.dhupia on 12/10/14.
 */
public class DEProcessor {
    private static final String ZEROTIME = "0001-01-01T00:00:00Z";
    private static Logger logger = LoggerFactory.getLogger(DEProcessor.class);
    private static Client client;
    private static AdUnitProcessor adUnitProcessor;
    private static VideoProcessor videoProcessor;

    @Inject
    public DEProcessor(Client esClient, AdUnitProcessor adUnitProcessor, VideoProcessor videoProcessor) {
        this.client = esClient;
        this.adUnitProcessor = adUnitProcessor;
        this.videoProcessor = videoProcessor;
    }


    public ItemsResponse recommend(SearchQueryRequest sq, Integer positions, String allowedTypes) throws DeException {
        List<VideoResponse> targetedVideos = null;
        List<AdUnitResponse> ads = null;
        List<? extends ItemsResponse> mergedList = null;
        ItemsResponse itemsResponse = new ItemsResponse();
        String[] at = StringUtils.split(allowedTypes, ",", 2);
        sq = DeHelper.modifySearchQueryReq(sq);

        if (at == null || at.length == 0) {
            ads = new AdQueryCommand(adUnitProcessor, sq, positions).execute();
            itemsResponse.setResponse(ads);
            return itemsResponse;

        }

        if (at.length == 1 && StringUtils.containsIgnoreCase(at[0], "promoted")) {
            ads = new AdQueryCommand(adUnitProcessor, sq, positions).execute();
            itemsResponse.setResponse(ads);
            return itemsResponse;

        }

        if (at.length == 1 && StringUtils.containsIgnoreCase(at[0], "organic")) {
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
        }

        if (at.length == 2
                && StringUtils.containsIgnoreCase(allowedTypes, "promoted")
                && StringUtils.containsIgnoreCase(allowedTypes, "organic")) {

            ads = new AdQueryCommand(adUnitProcessor, sq, positions).execute();
            targetedVideos = new VideoQueryCommand(videoProcessor, sq, positions).execute();
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
        boolean indexExists = client.admin()
                .indices()
                .prepareExists(DeHelper.getIndex())
                .execute()
                .actionGet()
                .isExists();
        boolean adUnitTypeExists = client.admin()
                .indices()
                .prepareTypesExists(DeHelper.getIndex())
                .setTypes(DeHelper.getAdUnitsType())
                .execute()
                .actionGet()
                .isExists();
        boolean videoTypeExists = client.admin()
                .indices()
                .prepareTypesExists(DeHelper.getIndex())
                .setTypes(DeHelper.getOrganicVideoType())
                .execute()
                .actionGet()
                .isExists();

        if (indexExists && adUnitTypeExists && videoTypeExists) {
            return client.admin()
                    .cluster()
                    .prepareHealth(DeHelper.getIndex())
                    .execute()
                    .actionGet();
        } else {
            throw new DeException(new Throwable("index or type does not exist"), 500);
        }
    }


    public void createIndexWithTypes() throws DeException {
        ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder()
                .put("node.name", DeHelper.getNode())
                .put("path.data", DeHelper.getDataDir())
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0);

        ESIndexTypeFactory.createIndex(client, DeHelper.getIndex(), elasticsearchSettings.build(), DeHelper.getAdUnitsType(), DeHelper.getOrganicVideoType());
    }


    public boolean deleteIndex() throws DeException {
        DeleteIndexRequestBuilder delIdx = client.admin().indices().prepareDelete(DeHelper.getIndex());
        return delIdx.execute().actionGet().isAcknowledged();
    }


    public boolean deleteById(String indexName, String type, String id) throws DeException {
        if (StringUtils.isBlank(indexName) || StringUtils.isBlank(type) || StringUtils.isBlank(id)) {
            return false;
        }
        return client.prepareDelete(indexName, type, id).execute().actionGet().isFound();
    }


    public String getLastUpdatedTimeStamp(String type) throws DeException {
        if (StringUtils.isBlank(type)) {
            throw new DeException(new Throwable("Type cannot be blank"), 400);
        }
        final String UPDATED = "_updated";

        QueryBuilder qb = QueryBuilders.matchAllQuery();

        SearchRequestBuilder srb1 = client.prepareSearch(DeHelper.getIndex())
                .setTypes(type)
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(qb)
                .setSize(1)
                .addSort(UPDATED, SortOrder.DESC)
                .addField(UPDATED);
        SearchResponse searchResponse = srb1.execute().actionGet();

        if (searchResponse != null && searchResponse.getHits() != null && searchResponse.getHits().getHits().length == 1) {
            return searchResponse.getHits().getHits()[0].field(UPDATED).getValue();
        }

        return ZEROTIME;
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
}
