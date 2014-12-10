package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.AdUnit;
import com.dailymotion.pixelle.deserver.model.AdUnitResponse;
import com.dailymotion.pixelle.deserver.model.ItemsResponse;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.providers.ESIndexTypeFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
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

    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private static Logger logger = LoggerFactory.getLogger(DEProcessorImpl.class);

    private static Client client;

    @Inject
    public DEProcessorImpl(Client esClient) {
        this.client = esClient;
    }

    @Override
    public ClusterHealthResponse getHealthCheck() {
        ActionFuture<ClusterHealthResponse> resp = client.admin().cluster().health(new ClusterHealthRequest(DeHelper.getIndex()));
        return resp.actionGet();
    }

    @Override
    public ItemsResponse recommend(SearchQueryRequest sq, Integer positions, String[] allowedTypes) throws DeException {
        if (positions == null || sq == null) {
            throw new DeException(new Throwable("no positions/searchquery provided in request body"), 400);
        }

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
            if (StringUtils.isBlank(sq.getTime())) {
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
            fb.must(FilterBuilders.termsFilter("categories", DeHelper.toLowerCase(sq.getCategories())));
            fb.must(FilterBuilders.termsFilter("languages", DeHelper.toLowerCase(sq.getLanguages())));
            fb.must(FilterBuilders.termsFilter("locations", DeHelper.toLowerCase(sq.getLocations())));
            fb.mustNot(FilterBuilders.termFilter("goal_reached", true));
            fb.mustNot(FilterBuilders.termFilter("paused", true));
            fb.must(FilterBuilders.rangeFilter("start_date").lte(sq.getTime()));
            fb.must(FilterBuilders.rangeFilter("end_date").gte(sq.getTime()));

            if (sq.getDevice() != null) {
                fb.must(FilterBuilders.termsFilter("devices", "all", sq.getDevice().toLowerCase()));
            } else {
                fb.must(FilterBuilders.termsFilter("devices", "all"));
            }
            if (sq.getFormat() != null) {
                fb.must(FilterBuilders.termsFilter("formats", "all", sq.getFormat().toLowerCase()));
            } else {
                fb.must(FilterBuilders.termsFilter("formats", "all"));
            }
            if (sq.getLocations() != null && sq.getLocations().size() > 0) {
                fb.mustNot(FilterBuilders.termsFilter("excluded_locations", DeHelper.toLowerCase(sq.getLocations())));
            }

            if (sq.getCategories() != null && sq.getCategories().size() > 0) {
                fb.mustNot(FilterBuilders.termsFilter("excluded_categories", DeHelper.toLowerCase(sq.getCategories())));
            }

            QueryBuilder qb = QueryBuilders.functionScoreQuery(fb)
                    .add(ScoreFunctionBuilders.randomFunction((int) (Math.random() * 100)));

            SearchRequestBuilder srb1 = client.prepareSearch(DeHelper.getIndex())
                    .setTypes(DeHelper.getAdUnitsType())
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(qb)
                    .setSize(positions * 4);


            logger.info(srb1.toString());

            SearchResponse searchResponse = srb1.execute().actionGet();
            List<AdUnitResponse> adUnitResponses = new ArrayList<AdUnitResponse>();

            for (SearchHit hit : searchResponse.getHits().getHits()) {
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
    public List<AdUnit> getAdUnitsByCampaign(String cid) {
        if (StringUtils.isBlank(cid)) {
            throw new DeException(new Throwable("no cid provided"), 400);
        }


        BoolFilterBuilder fb = FilterBuilders.boolFilter().must(FilterBuilders.termFilter("campaign", cid));
        QueryBuilder qb = QueryBuilders.filteredQuery(null, fb);
        SearchRequestBuilder srb1 = client.prepareSearch(DeHelper.getIndex())
                .setTypes(DeHelper.getAdUnitsType())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb);
        SearchResponse searchResponse = srb1.execute().actionGet();
        List<AdUnit> adUnits = new ArrayList<AdUnit>();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            AdUnit unit;
            try {
                unit = objectMapper.readValue(hit.getSourceAsString(), AdUnit.class);
            } catch (IOException e) {
                throw new DeException(e.getCause(), 500);
            }
            adUnits.add(unit);
        }
        if (adUnits.size() == 0) {
            return null;
        }
        return adUnits;
    }


    @Override
    public boolean insertAdUnit(AdUnit unit) throws DeException {
        if (unit == null) {
            throw new DeException(new Throwable("no adunit found in request body"), 400);
        }


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
        if (StringUtils.isBlank(unit.getStartDate())) {
            unit.setStartDate(DeHelper.timeToISO8601String(DeHelper.currentUTCTime()));
        }
        if (StringUtils.isBlank(unit.getEndDate())) {
            unit.setEndDate(DeHelper.timeToISO8601String(DeHelper.currentUTCTime().plusYears(100)));
        }

        unit.setCategories(DeHelper.stringListToLowerCase(unit.getCategories()));
        unit.setDevices(DeHelper.stringListToLowerCase(unit.getDevices()));
        unit.setExcludedCategories(DeHelper.stringListToLowerCase(unit.getExcludedCategories()));
        unit.setExcludedLocations(DeHelper.stringListToLowerCase(unit.getExcludedLocations()));
        unit.setFormats(DeHelper.stringListToLowerCase(unit.getFormats()));
        unit.setLanguages(DeHelper.stringListToLowerCase(unit.getLanguages()));
        unit.setLocations(DeHelper.stringListToLowerCase(unit.getLocations()));

        unit.setStatus(StringUtils.lowerCase(unit.getStatus()));

        return updateAdUnit(unit);
    }


    @Override
    public boolean updateAdUnit(AdUnit unit) throws DeException {
        if (unit == null) {
            throw new DeException(new Throwable("no adunit found in request body"), 400);
        }

        boolean result = false;
        try {
            result = client.prepareUpdate(DeHelper.getIndex(), DeHelper.getAdUnitsType(), unit.getId())
                    .setDoc(objectMapper.writeValueAsString(unit))
                    .setDocAsUpsert(true)
                    .execute()
                    .actionGet()
                    .isCreated();

        } catch (JsonProcessingException e) {
            logger.error("Error converting adunit to string", e);
            throw new DeException(e, 500);
        }
        return result;
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

        return DeHelper.timeToISO8601String(DateTime.now(DateTimeZone.UTC).minusYears(10));
    }

    @Override
    public AdUnit getAdUnitById(String id) throws DeException{
        if (StringUtils.isBlank(id)) {
            throw new DeException(new Throwable("id canot be blank"), 400);
        }
        GetResponse response = client.prepareGet(DeHelper.getIndex(), DeHelper.getAdUnitsType(), id).execute().actionGet();
        AdUnit unit = null;
        byte[] responseSourceAsBytes = response.getSourceAsBytes();
        if (response != null && responseSourceAsBytes != null) {
            try {
                unit = objectMapper.readValue(responseSourceAsBytes, AdUnit.class);
            } catch (IOException e) {
                logger.error("error parsing adunit", e);
                throw new DeException(e,500);
            }
        }
        return unit;
    }

    @Override
    public List<AdUnit> getAllAdUnits() {
        QueryBuilder qb = QueryBuilders.matchAllQuery();

        //TODO fix this later - we should do paging instead of getting all docs
        SearchRequestBuilder srb1 = client.prepareSearch(DeHelper.getIndex())
                .setTypes(DeHelper.getAdUnitsType())
                .setQuery(qb)
                .setSize(1000000);


        SearchResponse searchResponse = srb1.execute().actionGet();
        List<AdUnit> adUnits = new ArrayList<AdUnit>();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            AdUnit unit;
            try {
                unit = objectMapper.readValue(hit.getSourceAsString(), AdUnit.class);
            } catch (IOException e) {
                throw new DeException(e.getCause(), 500);
            }
            adUnits.add(unit);
        }
        if (adUnits.size() == 0) {
            return null;
        }
        return adUnits;
    }
}
