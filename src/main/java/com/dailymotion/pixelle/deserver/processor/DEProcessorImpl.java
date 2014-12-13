package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.*;
import com.dailymotion.pixelle.deserver.processor.hystrix.AdQueryCommand;
import com.dailymotion.pixelle.deserver.processor.hystrix.VideoQueryCommand;
import com.dailymotion.pixelle.deserver.providers.ESIndexTypeFactory;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
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
public class DEProcessorImpl implements DEProcessor {
    private static final String ZEROTIME = "0001-01-01T00:00:00Z";
    private static Logger logger = LoggerFactory.getLogger(DEProcessorImpl.class);
    private static Client client;
    private static AdUnitProcessor adUnitProcessor;
    private static VideoProcessor videoProcessor;

    @Inject
    public DEProcessorImpl(Client esClient, AdUnitProcessor adUnitProcessor, VideoProcessor videoProcessor) {
        this.client = esClient;
        this.adUnitProcessor = adUnitProcessor;
        this.videoProcessor = videoProcessor;
    }

    @Override
    public ItemsResponse recommend(SearchQueryRequest sq, Integer positions, String allowedTypes) throws DeException {
        List<VideoResponse> videos = null;
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
            videos = new VideoQueryCommand(videoProcessor, sq, positions).execute();
            mergedList = mergeAndFillList(null, videos, positions);
            itemsResponse.setResponse(mergedList);
            return itemsResponse;
        }

        if (at.length == 2
                && StringUtils.containsIgnoreCase(allowedTypes, "promoted")
                && StringUtils.containsIgnoreCase(allowedTypes, "organic")) {

            ads = new AdQueryCommand(adUnitProcessor, sq, positions).execute();
            videos = new VideoQueryCommand(videoProcessor, sq, positions).execute();
            mergedList = mergeAndFillList(ads, videos, positions);
            itemsResponse.setResponse(mergedList);
            return itemsResponse;
        }
        return itemsResponse;
    }

    @Override
    public List<AdUnit> getAdUnitsByCampaign(String cid) {
        return adUnitProcessor.getAdUnitsByCampaign(cid);
    }

    @Override
    public boolean insertAdUnit(AdUnit unit) throws DeException {
        return adUnitProcessor.insertAdUnit(unit);
    }

    @Override
    public boolean updateAdUnit(AdUnit unit) throws DeException {
        return adUnitProcessor.updateAdUnit(unit);
    }

    @Override
    public AdUnit getAdUnitById(String id) {
        return adUnitProcessor.getAdUnitById(id);
    }

    @Override
    public Video getVideoById(String id) throws DeException {
        return videoProcessor.getVideoById(id);
    }

    @Override
    public List<AdUnit> getAllAdUnits() {
        return adUnitProcessor.getAllAdUnits();
    }

    @Override
    public boolean insertVideo(Video video) throws DeException {
        return videoProcessor.insertVideo(video);
    }

    @Override
    public boolean updateVideo(Video video) throws DeException {
        return videoProcessor.updateVideo(video);
    }

    @Override
    public ClusterHealthResponse getHealthCheck() {
        ActionFuture<ClusterHealthResponse> resp = client.admin().cluster().health(new ClusterHealthRequest(DeHelper.getIndex()));
        return resp.actionGet();
    }

    @Override
    public Boolean createAdIndexWithType() throws DeException {
        ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder()
                .put("node.name", DeHelper.getNode())
                .put("path.data", DeHelper.getDataDir())
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0);

        ESIndexTypeFactory.createIndex(client, DeHelper.getIndex(), elasticsearchSettings.build(), DeHelper.getAdUnitsType());
        return true;
    }

    @Override
    public boolean deleteIndex() throws DeException {
        return client.admin().indices().delete(new DeleteIndexRequestBuilder(client.admin().indices(), DeHelper.getIndex())
                .request()).actionGet().isAcknowledged();
    }

    @Override
    public boolean deleteById(String indexName, String type, String id) throws DeException {
        if (StringUtils.isBlank(indexName) || StringUtils.isBlank(type) || StringUtils.isBlank(id)) {
            return false;
        }
        return client.prepareDelete(indexName, type, id).execute().actionGet().isFound();
    }

    @Override
    public String getLastUpdatedTimeStamp(String type) throws DeException {
        if (StringUtils.isBlank(type)) {
            throw new DeException(new Throwable("Type cannot be blank"), 400);
        }
        final String UPDATED = "_updated";

        QueryBuilder qb = QueryBuilders.matchAllQuery();

        SearchRequestBuilder srb1 = client.prepareSearch(DeHelper.getIndex())
                .setTypes(type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
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

    public List<? extends ItemsResponse> mergeAndFillList(final List<AdUnitResponse> ads, final List<VideoResponse> targetedVideos, final Integer positions) {

        if (ads == null && targetedVideos.size() == positions) {
            return targetedVideos;
        }
        String pattern = DeHelper.getWidgetPattern();
        int len = pattern.length();

        List<ItemsResponse> items = new ArrayList<ItemsResponse>();
        List<VideoResponse> untargetedVideos = new ArrayList<VideoResponse>();
        int adIter = 0;
        int videoIter = 0;
        int patternIter = 0;
        int positionsFilled = 0;
        boolean skip = false;

        while (positionsFilled < positions) {
            if (ads != null && ads.size() > adIter && (pattern.charAt(patternIter % len) == 'P' || pattern.charAt(patternIter % len) == 'p')) {
                items.add(ads.get(adIter++));
                patternIter++;
                positionsFilled++;
            } else if (targetedVideos != null && targetedVideos.size() > videoIter && (pattern.charAt(patternIter % len) == 'O' || pattern.charAt(patternIter % len) == 'o')) {
                items.add(targetedVideos.get(videoIter++));
                patternIter++;
                positionsFilled++;
            } else if (targetedVideos != null && targetedVideos.size() > videoIter) { // not enough ads, so fill with all available targeted videos
                items.add(targetedVideos.get(videoIter++));
                patternIter++;
                positionsFilled++;
            } else if (!skip && (targetedVideos == null || targetedVideos.size() == videoIter)) { //not enough targeted videos, so fill it with un-targeted videos
                VideoResponse untargetedVideo = videoProcessor.getDistinctUntargetedVideo(targetedVideos, untargetedVideos);
                if (untargetedVideo != null) {
                    untargetedVideos.add(untargetedVideo);
                    items.add(untargetedVideo);
                    patternIter++;
                    positionsFilled++;
                } else {
                    skip = true;
                }
            } else if (ads != null && ads.size() > adIter) { //not enough targeted videos, so fill with targeted ads - rare case
                items.add(ads.get(adIter++));
                patternIter++;
                positionsFilled++;
            } else { // don't bother filling with un-targeted ads
                break;
            }
        }

        return items;
    }
}
