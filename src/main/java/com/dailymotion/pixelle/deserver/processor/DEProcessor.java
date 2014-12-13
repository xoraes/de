package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.AdUnit;
import com.dailymotion.pixelle.deserver.model.ItemsResponse;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.model.Video;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;

import java.util.List;

/**
 * Created by n.dhupia on 12/10/14.
 */
public interface DEProcessor {
    ItemsResponse recommend(SearchQueryRequest sq, Integer positions, String allowedTypes) throws DeException;

    List<AdUnit> getAdUnitsByCampaign(String cid);

    boolean insertAdUnit(AdUnit unit) throws DeException;

    boolean updateAdUnit(AdUnit unit) throws DeException;

    AdUnit getAdUnitById(String id);

    Video getVideoById(String id) throws DeException;

    List<AdUnit> getAllAdUnits();

    boolean insertVideo(Video video) throws DeException;

    public boolean updateVideo(Video video) throws DeException;

    ClusterHealthResponse getHealthCheck();

    Boolean createAdIndexWithType() throws DeException;

    boolean deleteIndex() throws DeException;

    boolean deleteById(String indexName, String type, String id) throws DeException;

    String getLastUpdatedTimeStamp(String type) throws DeException;

}
