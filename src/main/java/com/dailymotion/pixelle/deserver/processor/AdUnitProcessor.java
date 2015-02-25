package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.AdUnit;
import com.dailymotion.pixelle.deserver.model.AdUnitResponse;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.BasicCounter;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.MonitorConfig;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.http.HttpStatus;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
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
public class AdUnitProcessor {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Integer MAX_YEARS = 100;
    private static final Integer MAX_RANDOM = 100;
    private static final Integer SIZ_MULTIPLIER = 4;
    private static Logger logger = LoggerFactory.getLogger(AdUnitProcessor.class);
    private static Client client;
    // JMX: com.netflix.servo.COUNTER.TotalAdsRequestsServed
    private static Counter totalAdsRequestsServed = new BasicCounter(MonitorConfig
            .builder("TotalAdsRequestsServed").build());

    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        DefaultMonitorRegistry.getInstance().register(totalAdsRequestsServed);
    }

    @Inject
    public AdUnitProcessor(Client esClient) {
        client = esClient;
    }

    public List<AdUnitResponse> recommend(SearchQueryRequest sq, Integer positions) throws DeException {

        List<AdUnitResponse> adUnitResponses = null;
        if (sq != null) {
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
            fb.mustNot(FilterBuilders.termFilter("paused", true));
            fb.must(FilterBuilders.rangeFilter("start_date").lte(sq.getTime()));
            fb.must(FilterBuilders.rangeFilter("end_date").gte(sq.getTime()));

            if (StringUtils.isNotBlank(sq.getDevice())) {
                fb.must(FilterBuilders.termsFilter("devices", "all", sq.getDevice().toLowerCase()));
            } else {
                fb.must(FilterBuilders.termsFilter("devices", "all"));
            }
            if (StringUtils.isNotBlank(sq.getFormat())) {
                fb.must(FilterBuilders.termsFilter("formats", "all", sq.getFormat().toLowerCase()));
            } else {
                fb.must(FilterBuilders.termsFilter("formats", "all"));
            }
            if (!DeHelper.isEmptyList(sq.getLocations())) {
                fb.mustNot(FilterBuilders.termsFilter("excluded_locations", DeHelper.toLowerCase(sq.getLocations())));
            }

            if (!DeHelper.isEmptyList(sq.getCategories())) {
                fb.mustNot(FilterBuilders.termsFilter("excluded_categories", DeHelper.toLowerCase(sq.getCategories())));
            }

            fb.must(FilterBuilders.orFilter(FilterBuilders.missingFilter("goal_views"),
                    FilterBuilders.missingFilter("views"),
                    FilterBuilders.scriptFilter("doc['views'].value < doc['goal_views'].value").lang("expression")));

            QueryBuilder qb = QueryBuilders.functionScoreQuery(fb)
                    .add(ScoreFunctionBuilders.randomFunction((int) (Math.random() * MAX_RANDOM)));


            SearchRequestBuilder srb1 = client.prepareSearch(DeHelper.getPromotedIndex())
                    .setTypes(DeHelper.getAdUnitsType())
                    .setSearchType(SearchType.DFS_QUERY_AND_FETCH)
                    .setQuery(qb)
                    .setSize(positions * SIZ_MULTIPLIER);

            logger.info(srb1.toString());


            SearchResponse searchResponse = srb1.execute().actionGet();
            adUnitResponses = new ArrayList<AdUnitResponse>();

            for (SearchHit hit : searchResponse.getHits().getHits()) {
                AdUnitResponse unit;
                try {
                    unit = OBJECT_MAPPER.readValue(hit.getSourceAsString(), AdUnitResponse.class);
                } catch (IOException e) {
                    throw new DeException(e.getCause(), HttpStatus.INTERNAL_SERVER_ERROR_500);
                }
                adUnitResponses.add(unit);
            }
            adUnitResponses = DeHelper.removeDuplicateCampaigns(positions, adUnitResponses);
            logger.info("Num responses:" + adUnitResponses.size());

        }
        if (DeHelper.isEmptyList(adUnitResponses)) {
            logger.info("No ads returned =======> " + sq.toString());
        } else {
            logger.info("Success =======> " + adUnitResponses.toString());
            totalAdsRequestsServed.increment();
        }
        return adUnitResponses;
    }

    public List<AdUnit> getAdUnitsByCampaign(String cid) {
        if (StringUtils.isBlank(cid)) {
            throw new DeException(new Throwable("no cid provided"), HttpStatus.BAD_REQUEST_400);
        }


        BoolFilterBuilder fb = FilterBuilders.boolFilter().must(FilterBuilders.termFilter("campaign", cid));
        QueryBuilder qb = QueryBuilders.filteredQuery(null, fb);
        SearchRequestBuilder srb1 = client.prepareSearch(DeHelper.getPromotedIndex())
                .setTypes(DeHelper.getAdUnitsType())
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(qb);
        SearchResponse searchResponse = srb1.execute().actionGet();
        List<AdUnit> adUnits = new ArrayList<AdUnit>();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            AdUnit unit;
            try {
                unit = OBJECT_MAPPER.readValue(hit.getSourceAsString(), AdUnit.class);
            } catch (IOException e) {
                throw new DeException(e.getCause(), HttpStatus.INTERNAL_SERVER_ERROR_500);
            }
            adUnits.add(unit);
        }
        if (DeHelper.isEmptyList(adUnits)) {
            return null;
        }
        return adUnits;
    }

    public void insertAdUnit(AdUnit unit) throws DeException {
        if (unit == null) {
            throw new DeException(new Throwable("no adunit found in request body"), HttpStatus.BAD_REQUEST_400);
        }
        updateAdUnit(modifyAdUnitForInsert(unit));
    }

    private AdUnit modifyAdUnitForInsert(AdUnit unit) {
        if (DeHelper.isEmptyList(unit.getLocations())) {
            unit.setLocations(Arrays.asList("all"));
        }
        if (DeHelper.isEmptyList(unit.getLanguages())) {
            unit.setLanguages(Arrays.asList("all"));
        }

        if (DeHelper.isEmptyList(unit.getDevices())) {
            unit.setDevices(Arrays.asList("all"));
        }
        if (DeHelper.isEmptyList(unit.getCategories())) {
            unit.setCategories(Arrays.asList("all"));
        }
        if (DeHelper.isEmptyList(unit.getFormats())) {
            unit.setFormats(Arrays.asList("all"));
        }

        List<String> sch = DeHelper.convertSchedulesToTimeTable(unit.getSchedules());

        if (DeHelper.isEmptyList(sch)) {
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
            unit.setEndDate(DeHelper.timeToISO8601String(DeHelper.currentUTCTime().plusYears(MAX_YEARS)));
        }

        unit.setCategories(DeHelper.stringListToLowerCase(unit.getCategories()));
        unit.setDevices(DeHelper.stringListToLowerCase(unit.getDevices()));
        unit.setExcludedCategories(DeHelper.stringListToLowerCase(unit.getExcludedCategories()));
        unit.setExcludedLocations(DeHelper.stringListToLowerCase(unit.getExcludedLocations()));
        unit.setFormats(DeHelper.stringListToLowerCase(unit.getFormats()));
        unit.setLanguages(DeHelper.stringListToLowerCase(unit.getLanguages()));
        unit.setLocations(DeHelper.stringListToLowerCase(unit.getLocations()));

        unit.setStatus(StringUtils.lowerCase(unit.getStatus()));
        return unit;
    }

    public void updateAdUnit(AdUnit unit) throws DeException {
        if (unit == null) {
            throw new DeException(new Throwable("no adunit found in request body"), HttpStatus.BAD_REQUEST_400);
        }

        try {
            client.prepareUpdate(DeHelper.getPromotedIndex(), DeHelper.getAdUnitsType(), unit.getId())
                    .setDoc(OBJECT_MAPPER.writeValueAsString(unit))
                    .setDocAsUpsert(true)
                    .execute()
                    .actionGet();
        } catch (JsonProcessingException e) {
            throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
        } catch (ElasticsearchException e) {
            throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
        }
    }

    public void insertAdUnitsInBulk(List<AdUnit> adUnits) throws DeException {
        if (DeHelper.isEmptyList(adUnits)) {
            return;
        }
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        logger.info("Bulk loading:" + adUnits.size() + " ads");


        for (AdUnit adUnit : adUnits) {
            adUnit = modifyAdUnitForInsert(adUnit);
            logger.info("Loading adunit" + adUnit.toString());
            try {
                bulkRequest.add(client.prepareUpdate(DeHelper.getPromotedIndex(), DeHelper.getAdUnitsType(), adUnit.getId())
                        .setDoc(OBJECT_MAPPER.writeValueAsString(adUnit))
                        .setDocAsUpsert(true));
            } catch (JsonProcessingException e) {
                logger.error("Error converting adunit to string", e);
                throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
            }
        }

        BulkResponse bulkResponse;
        try {
            bulkResponse = bulkRequest.execute().actionGet();
        } catch (ElasticsearchException e) {
            throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
        }
        if (bulkResponse != null && bulkResponse.hasFailures()) {
            logger.error("Error Bulk loading:" + adUnits.size() + " adUnits");
            while (bulkResponse.iterator().hasNext()) {
                BulkItemResponse br = bulkResponse.iterator().next();
                if (br.isFailed()) {
                    logger.error(br.getFailureMessage());
                }

            }
            // process failures by iterating through each bulk response item
            throw new DeException(new Throwable("Error inserting adunits in Bulk"), HttpStatus.INTERNAL_SERVER_ERROR_500);
        }
    }

    public AdUnit getAdUnitById(String id) throws DeException {
        if (StringUtils.isBlank(id)) {
            throw new DeException(new Throwable("id cannot be blank"), HttpStatus.BAD_REQUEST_400);
        }
        GetResponse response = client.prepareGet(DeHelper.getPromotedIndex(), DeHelper.getAdUnitsType(), id).execute().actionGet();
        AdUnit unit = null;
        byte[] responseSourceAsBytes = response.getSourceAsBytes();
        if (response != null && responseSourceAsBytes != null) {
            try {
                unit = OBJECT_MAPPER.readValue(responseSourceAsBytes, AdUnit.class);
            } catch (IOException e) {
                logger.error("error parsing adunit", e);
                throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
            }
        }
        return unit;
    }

    public List<AdUnit> getAllAdUnits() {
        QueryBuilder qb = QueryBuilders.matchAllQuery();

        //TODO fix this later - we should do paging instead of getting all docs
        SearchRequestBuilder srb1 = client.prepareSearch(DeHelper.getPromotedIndex())
                .setTypes(DeHelper.getAdUnitsType())
                .setQuery(qb)
                .setSize(1000000);


        SearchResponse searchResponse = srb1.execute().actionGet();
        List<AdUnit> adUnits = new ArrayList<AdUnit>();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            AdUnit unit;
            try {
                unit = OBJECT_MAPPER.readValue(hit.getSourceAsString(), AdUnit.class);
            } catch (IOException e) {
                throw new DeException(e.getCause(), HttpStatus.INTERNAL_SERVER_ERROR_500);
            }
            adUnits.add(unit);
        }
        if (DeHelper.isEmptyList(adUnits)) {
            return null;
        }
        return adUnits;
    }
}
