package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.AdUnit;
import com.dailymotion.pixelle.deserver.model.AdUnitResponse;
import com.dailymotion.pixelle.deserver.model.ItemsResponse;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.eclipse.jetty.util.StringUtil;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by n.dhupia on 11/3/14.
 */
public class DEProcessorImpl implements DEProcessor {
    private static Logger logger = LoggerFactory.getLogger(DEProcessorImpl.class);

    private static Client client;

    @Inject
    public DEProcessorImpl(Client esClient) {
        this.client = esClient;
    }

    @Override
    public ItemsResponse recommend(SearchQueryRequest sq, int positions, String[] allowedTypes) throws DeException {
        ItemsResponse itemsResponse = new ItemsResponse();
        if (sq != null) {
            if (DeHelper.isEmptyArray(sq.getCategories())) {
                sq.setCategories(Arrays.asList("all"));
            } else {
                sq.getCategories().add("all");
            }
            if (DeHelper.isEmptyArray(sq.getLocations())) {
                sq.setLocations(Arrays.asList("all"));
            } else {
                sq.getLocations().add("all");
            }
            if (DeHelper.isEmptyArray(sq.getLanguages())) {
                sq.setLanguages(Arrays.asList("all"));
            } else {
                sq.getLanguages().add("all");
            }
            if (StringUtil.isBlank(sq.getTime())) {
                sq.setTime(DeHelper.timeToISO8601String(DeHelper.currentUTCTime()));
            }

            DateTimeFormatter df = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ");
            //If withOffsetParsed() is not used then everything is converted to local time
            //resulting in dayOfWeek and HourOfDay to be incorrect
            DateTime dt = df.withOffsetParsed().parseDateTime(sq.getTime());
            String timetable = dt.dayOfWeek().getAsText().toLowerCase() + ":" + dt.getHourOfDay() + ":" + "true";

            BoolFilterBuilder fb = FilterBuilders.boolFilter();
            fb.must(FilterBuilders.termFilter("status", "active"));
            fb.must(FilterBuilders.termFilter("timetable", timetable));
            fb.must(FilterBuilders.termsFilter("categories", sq.getCategories()));
            fb.must(FilterBuilders.termsFilter("languages", sq.getLanguages()));
            fb.must(FilterBuilders.termsFilter("locations", sq.getLocations()));
            fb.mustNot(FilterBuilders.termFilter("goal_reached", true));
            fb.mustNot(FilterBuilders.termFilter("paused", true));
            fb.must(FilterBuilders.rangeFilter("start_date").lte(sq.getTime()));
            fb.must(FilterBuilders.rangeFilter("end_date").gte(sq.getTime()));

            if (sq.getDevice() != null) {
                fb.must(FilterBuilders.termsFilter("devices", "all", sq.getDevice()));
            } else {
                fb.must(FilterBuilders.termsFilter("devices", "all"));
            }
            if (sq.getFormat() != null) {
                fb.must(FilterBuilders.termsFilter("formats", "all", sq.getFormat()));
            } else {
                fb.must(FilterBuilders.termsFilter("formats", "all"));
            }
            if (sq.getLocations() != null && sq.getLocations().size() > 0) {
                fb.mustNot(FilterBuilders.termsFilter("excluded_locations", sq.getLocations()));
            }

            if (sq.getCategories() != null && sq.getCategories().size() > 0) {
                fb.mustNot(FilterBuilders.termsFilter("excluded_categories", sq.getCategories()));
            }

            QueryBuilder qb = QueryBuilders.functionScoreQuery(fb)
                    .add(ScoreFunctionBuilders.randomFunction((int) (Math.random() * 100)));

            SearchRequestBuilder srb1 = client.prepareSearch("pixelle")
                    .setTypes("adunits")
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(qb)
                    .setSize(positions * 4);


            logger.info(srb1.toString());

            SearchResponse searchResponse = srb1.execute().actionGet();
            List<AdUnitResponse> adUnitResponses = new ArrayList<AdUnitResponse>();

            for (SearchHit hit : searchResponse.getHits().getHits()) {
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                AdUnitResponse unit;
                try {
                    unit = objectMapper.readValue(hit.getSourceAsString(), AdUnitResponse.class);
                } catch (IOException e) {
                    throw new DeException(e.getCause(), 500);
                }
                adUnitResponses.add(unit);
            }
            logger.info("Num responses:" + adUnitResponses.size());
            adUnitResponses = DeHelper.removeDuplicateCampaigns(positions, adUnitResponses);
            itemsResponse.setAdUnitResponse(adUnitResponses);
        }
        return itemsResponse;
    }


    @Override
    public boolean insertAdUnit(AdUnit unit) throws DeException {
        if (unit.getLocations() == null || unit.getLocations().size() <= 0) {
            unit.setLocations(Arrays.asList("all"));
        }

        if (unit.getLanguages() == null || unit.getLanguages().size() <= 0) {
            unit.setLanguages(Arrays.asList("all"));
        }

        if (unit.getDevices() == null || unit.getDevices().size() <= 0) {
            unit.setDevices(Arrays.asList("all"));
        }
        if (unit.getCategories() == null || unit.getCategories().size() <= 0) {
            unit.setCategories(Arrays.asList("all"));
        }
        if (unit.getFormats() == null || unit.getFormats().size() <= 0) {
            unit.setFormats(Arrays.asList("all"));
        }

        List<String> sch = DeHelper.convertSchedulesToTimeTable(unit.getSchedules());

        if (DeHelper.isEmptyArray(sch)) {
            unit.setTimetable(new ArrayList<String>(Arrays.asList("all")));
        }
        unit.setTimetable(DeHelper.convertSchedulesToTimeTable(unit.getSchedules()));
        unit.setSchedules(null);
        if (unit.getCpc() == null || unit.getCpc() == 0) {
            unit.setCpc(0L);
        }
        if (StringUtil.isBlank(unit.getStartDate())) {
            unit.setStartDate(DeHelper.timeToISO8601String(DeHelper.currentUTCTime()));
        }
        if (StringUtil.isBlank(unit.getEndDate())) {
            unit.setEndDate(DeHelper.timeToISO8601String(DeHelper.currentUTCTime().plusYears(100)));
        }

        unit.setCategories(DeHelper.stringListToLowerCase(unit.getCategories()));
        unit.setDevices(DeHelper.stringListToLowerCase(unit.getDevices()));
        unit.setExcludedCategories(DeHelper.stringListToLowerCase(unit.getExcludedCategories()));
        unit.setExcludedLocations(DeHelper.stringListToLowerCase(unit.getExcludedLocations()));
        unit.setFormats(DeHelper.stringListToLowerCase(unit.getFormats()));
        unit.setLanguages(DeHelper.stringListToLowerCase(unit.getLanguages()));
        unit.setLocations(DeHelper.stringListToLowerCase(unit.getLocations()));
        unit.setStatus(unit.getStatus().toLowerCase());

        return updateAdUnit(unit);
    }


    @Override
    public boolean updateAdUnit(AdUnit unit) throws DeException {
        ObjectMapper mapper = new ObjectMapper();
        boolean result = false;
        try {
            result = client.prepareUpdate(DeHelper.getIndex(), DeHelper.getAdUnitsType(), unit.getId())
                    .setDoc(mapper.writeValueAsString(unit)).setDocAsUpsert(true).execute().actionGet().isCreated();
        } catch (JsonProcessingException e) {
            logger.error("Error converting adunit to string", e);
            throw new DeException(e, 500);
        }
        return result;
    }

    @Override
    public boolean deleteIndex(String indexName) throws DeException {
        return client.admin().indices().delete(new DeleteIndexRequestBuilder(client.admin().indices(), indexName)
                .request()).actionGet().isAcknowledged();
    }

    @Override
    public boolean deleteById(String indexName, String type, String id) throws DeException {
        return client.prepareDelete(indexName, type, id).execute().actionGet().isFound();

    }
}
