package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.AdUnit;
import com.dailymotion.pixelle.deserver.model.AdUnitResponse;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.BasicCounter;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.MonitorConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.Explanation;
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
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by n.dhupia on 11/3/14.
 */
public class AdUnitProcessor {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Integer MAX_YEARS = 100;
    private static final Integer SIZ_MULTIPLIER = 4;
    private static final Logger logger = LoggerFactory.getLogger(AdUnitProcessor.class);
    // JMX: com.netflix.servo.COUNTER.TotalAdsRequestsServed
    private static final Counter totalAdsRequestsServed = new BasicCounter(MonitorConfig
            .builder("TotalAdsRequestsServed").build());
    private static final DynamicStringProperty ctrScriptFunction =
            DynamicPropertyFactory.getInstance().getStringProperty("ctr.script.code", "");
    private static final DynamicStringProperty ctrScriptLang =
            DynamicPropertyFactory.getInstance().getStringProperty("ctr.script.lang", "expression");
    private static Client client;

    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        DefaultMonitorRegistry.getInstance().register(totalAdsRequestsServed);
    }

    @Inject
    public AdUnitProcessor(Client esClient) {
        client = esClient;
    }

    public static List<AdUnitResponse> recommend(SearchQueryRequest sq, Integer positions) throws DeException {
        List<AdUnitResponse> adUnitResponses = null;
        if (sq != null) {
            DateTimeFormatter df = DateTimeFormat.forPattern(DeHelper.getDateTimeFormatString());
            //If withOffsetParsed() is not used then everything is converted to local time
            //resulting in dayOfWeek and HourOfDay to be incorrect
            DateTime dt = df.withOffsetParsed().parseDateTime(sq.getTime());
            String timetable = dt.dayOfWeek().getAsText().toLowerCase() + ":" + dt.getHourOfDay() + ":" + "false";

            BoolFilterBuilder fb = FilterBuilders.boolFilter();
            fb.mustNot(FilterBuilders.termFilter("timetable", timetable));
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
                    .add(FilterBuilders.andFilter(FilterBuilders.rangeFilter("clicks").from(0), FilterBuilders.rangeFilter("impressions").from(0)),
                            ScoreFunctionBuilders.scriptFunction(ctrScriptFunction.getValue()).lang(ctrScriptLang.getValue()))
                    .add(ScoreFunctionBuilders.fieldValueFactorFunction("cpv").setWeight(2.0f));

            List<String> excludedAds = sq.getExcludedVideoIds();
            if (!DeHelper.isEmptyList(excludedAds)) {
                for (String id : excludedAds) {
                    fb.mustNot(FilterBuilders.termsFilter("video_id", id));
                }
            }

            SearchRequestBuilder srb1 = client.prepareSearch(DeHelper.promotedIndex.get())
                    .setTypes(DeHelper.adunitsType.get())
                    .setSearchType(SearchType.QUERY_AND_FETCH)
                    .setQuery(qb)
                    .setSize(positions * SIZ_MULTIPLIER);

            if (sq.isDebugEnabled()) {
                srb1.setExplain(true);
            }

            logger.info(srb1.toString());


            SearchResponse searchResponse = srb1.execute().actionGet();
            adUnitResponses = new ArrayList<AdUnitResponse>();

            for (SearchHit hit : searchResponse.getHits().getHits()) {
                try {
                    AdUnitResponse unit = OBJECT_MAPPER.readValue(hit.getSourceAsString(), AdUnitResponse.class);
                    if (sq.isDebugEnabled()) {
                        Explanation ex = new Explanation();
                        ex.setValue(hit.getScore());
                        ex.setDescription("Source ====>" + hit.getSourceAsString());
                        ex.addDetail(hit.explanation());
                        unit.setDebugInfo(ex.toHtml().replace("\n", ""));
                        logger.info(ex.toString());
                    }
                    adUnitResponses.add(unit);
                } catch (IOException e) {
                    throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
                }
            }
            adUnitResponses = removeDuplicateCampaigns(positions, adUnitResponses);
            logger.info("Num responses:" + adUnitResponses.size());

        }
        if (DeHelper.isEmptyList(adUnitResponses)) {
            //instead of returning no ads, return something even excluded-ad
            if (!DeHelper.isEmptyList(sq.getExcludedVideoIds())) {
                sq.setExcludedVideoIds(null);
                return recommend(sq, positions);
            } else {
                logger.info("No ads returned =======> " + sq.toString());
            }

        } else {
            logger.info("Success =======> " + (adUnitResponses != null ? adUnitResponses.toString() : null));
            totalAdsRequestsServed.increment();
        }
        return adUnitResponses;
    }

    public static List<AdUnit> getAdUnitsByCampaign(String cid) throws DeException {
        if (StringUtils.isBlank(cid)) {
            throw new DeException(new Throwable("no cid provided"), HttpStatus.BAD_REQUEST_400);
        }
        BoolFilterBuilder fb = FilterBuilders.boolFilter().must(FilterBuilders.termFilter("campaign", cid));
        QueryBuilder qb = QueryBuilders.filteredQuery(null, fb);
        SearchRequestBuilder srb1 = client.prepareSearch(DeHelper.promotedIndex.get())
                .setTypes(DeHelper.adunitsType.get())
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

    public static void insertAdUnit(AdUnit unit) throws DeException {
        if (unit == null) {
            throw new DeException(new Throwable("no adunit found in request body"), HttpStatus.BAD_REQUEST_400);
        }
        updateAdUnit(modifyAdUnitForInsert(unit), true);
    }

    private static AdUnit modifyAdUnitForInsert(AdUnit unit) {
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

        List<String> sch = convertSchedulesToTimeTable(unit.getSchedules());

        if (DeHelper.isEmptyList(sch)) {
            unit.setTimetable(new ArrayList<String>(Arrays.asList("all")));
        }
        unit.setTimetable(sch);
        unit.setSchedules(null);
        if (unit.getCpc() == null || unit.getCpc() == 0) {
            unit.setCpc(0L);
        }
        if (unit.getCpv() == null || unit.getCpv() == 0) {
            unit.setCpv(0L);
        }
        if (StringUtils.isBlank(unit.getStartDate())) {
            unit.setStartDate(DeHelper.timeToISO8601String(DeHelper.currentUTCTime()));
        }
        if (StringUtils.isBlank(unit.getEndDate())) {
            unit.setEndDate(DeHelper.timeToISO8601String(DeHelper.currentUTCTime().plusYears(MAX_YEARS)));
        }

        unit.setCategories(DeHelper.toLowerCase(unit.getCategories()));
        unit.setDevices(DeHelper.toLowerCase(unit.getDevices()));
        unit.setExcludedCategories(DeHelper.toLowerCase(unit.getExcludedCategories()));
        unit.setExcludedLocations(DeHelper.toLowerCase(unit.getExcludedLocations()));
        unit.setFormats(DeHelper.toLowerCase(unit.getFormats()));
        unit.setLanguages(DeHelper.toLowerCase(unit.getLanguages()));
        unit.setLocations(DeHelper.toLowerCase(unit.getLocations()));

        return unit;
    }

    public static void updateAdUnit(AdUnit unit, boolean enableUpsert) throws DeException {
        if (unit == null) {
            throw new DeException(new Throwable("no adunit found in request"), HttpStatus.BAD_REQUEST_400);
        }
        try {
            client.prepareUpdate(DeHelper.promotedIndex.get(), DeHelper.adunitsType.get(), unit.getId())
                    .setDoc(OBJECT_MAPPER.writeValueAsString(unit))
                    .setDocAsUpsert(enableUpsert)
                    .setRetryOnConflict(DeHelper.retryOnConflictAdUnits.get())
                    .execute()
                    .actionGet();
        } catch (JsonProcessingException e) {
            throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
        } catch (ElasticsearchException e) {
            throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
        }
    }

    public static void insertAdUnitsInBulk(List<AdUnit> adUnits) throws DeException {
        if (DeHelper.isEmptyList(adUnits)) {
            return;
        }
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        logger.info("Bulk loading:" + adUnits.size() + " ads");


        for (AdUnit adUnit : adUnits) {
            if (adUnit.getCpv() == null || adUnit.getCpv() == 0L) {
                logger.warn("failed inserting adunit due to no cpv: " + adUnit.toString());
                continue;
            }
            adUnit = modifyAdUnitForInsert(adUnit);
            logger.info("Loading adunit" + adUnit.toString());
            try {
                bulkRequest.add(client.prepareUpdate(DeHelper.promotedIndex.get(), DeHelper.adunitsType.get(), adUnit.getId())
                        .setDoc(OBJECT_MAPPER.writeValueAsString(adUnit))
                        .setRetryOnConflict(DeHelper.retryOnConflictAdUnits.get())
                        .setDocAsUpsert(true));
            } catch (JsonProcessingException e) {
                logger.error("Error converting adunit to string", e);
                throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
            }
        }

        if (bulkRequest.numberOfActions() > 0) {
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
    }

    public static AdUnit getAdUnitById(String id) throws DeException {
        if (StringUtils.isBlank(id)) {
            throw new DeException(new Throwable("id cannot be blank"), HttpStatus.BAD_REQUEST_400);
        }
        GetResponse response = client.prepareGet(DeHelper.promotedIndex.get(), DeHelper.adunitsType.get(), id).execute().actionGet();
        AdUnit unit = null;
        byte[] responseSourceAsBytes = response.getSourceAsBytes();
        if (responseSourceAsBytes != null) {
            try {
                unit = OBJECT_MAPPER.readValue(responseSourceAsBytes, AdUnit.class);
            } catch (IOException e) {
                logger.error("error parsing adunit", e);
                throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
            }
        }
        return unit;
    }

    public static List<AdUnit> getAllAdUnits() throws DeException {
        QueryBuilder qb = QueryBuilders.matchAllQuery();

        //TODO fix this later - we should do paging instead of getting all docs
        SearchRequestBuilder srb1 = client.prepareSearch(DeHelper.promotedIndex.get())
                .setTypes(DeHelper.adunitsType.get())
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

    private static List<AdUnitResponse> removeDuplicateCampaigns(int positions, List<AdUnitResponse> units) {
        int count = 1;
        Map<String, Integer> m = new HashMap<String, Integer>();
        List<AdUnitResponse> uniqueAds = new ArrayList<AdUnitResponse>();

        for (AdUnitResponse unit : units) {
            if (count > positions) {
                break;
            }
            if (!m.containsKey(unit.getCampaignId())) {
                m.put(unit.getCampaignId(), 1);
                uniqueAds.add(unit);
                count++;
            }
        }
        return uniqueAds;
    }

    private static Boolean isHourSet(int hour, int mask) {
        return (mask & (1 << hour)) > 0;
    }

    private static List<String> convertSchedulesToTimeTable(Integer[] schedules) {
        String day;
        Boolean hourSet;
        List<String> timeTable = null;
        if (schedules != null && schedules.length > 0) {
            timeTable = new ArrayList<>();
            for (int i = 0; i < 7; i++) {
                LocalDate date = new LocalDate();
                if (i == 0) {
                    date = date.withDayOfWeek(7);
                } else {
                    date = date.withDayOfWeek(i);
                }
                day = date.dayOfWeek().getAsText().toLowerCase();
                for (int j = 0; j < 24; j++) {
                    hourSet = isHourSet(j, schedules[i]);
                    if (!hourSet) {
                        timeTable.add(day + ":" + j + ":" + hourSet.toString());
                    }
                }
            }
        }
        return timeTable;
    }
}
