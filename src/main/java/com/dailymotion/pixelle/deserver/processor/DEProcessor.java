package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.AdUnit;
import com.dailymotion.pixelle.deserver.model.ItemsResponse;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;

/**
 * Created by n.dhupia on 11/4/14.
 */
public interface DEProcessor {
    ItemsResponse recommend(SearchQueryRequest sq, int positions, String[] allowedTypes) throws DeException;

    boolean insertAdUnit(AdUnit unit) throws DeException;

    boolean updateAdUnit(AdUnit unit) throws DeException;

    boolean deleteIndex(String indexName) throws DeException;

    boolean deleteById(String indexName, String type, String id) throws DeException;
}
