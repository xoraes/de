package com.dailymotion.pixelle.de.processor;

import com.dailymotion.pixelle.de.model.AdUnit;
import com.dailymotion.pixelle.de.model.AdUnitResponse;
import com.dailymotion.pixelle.de.model.SearchQueryRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.netflix.config.DynamicStringProperty;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.BasicCounter;
import com.netflix.servo.monitor.Counter;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.dailymotion.pixelle.de.processor.DeHelper.FORMAT.INWIDGET;
import static com.dailymotion.pixelle.de.processor.DeHelper.adunitsType;
import static com.dailymotion.pixelle.de.processor.DeHelper.currentUTCTime;
import static com.dailymotion.pixelle.de.processor.DeHelper.getDateTimeFormatString;
import static com.dailymotion.pixelle.de.processor.DeHelper.isEmptyList;
import static com.dailymotion.pixelle.de.processor.DeHelper.promotedIndex;
import static com.dailymotion.pixelle.de.processor.DeHelper.retryOnConflictAdUnits;
import static com.dailymotion.pixelle.de.processor.DeHelper.timeToISO8601String;
import static com.dailymotion.pixelle.de.processor.DeHelper.toLowerCase;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.netflix.config.DynamicPropertyFactory.getInstance;
import static com.netflix.servo.monitor.MonitorConfig.builder;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.eclipse.jetty.http.HttpStatus.BAD_REQUEST_400;
import static org.eclipse.jetty.http.HttpStatus.INTERNAL_SERVER_ERROR_500;
import static org.elasticsearch.action.search.SearchType.QUERY_AND_FETCH;
import static org.elasticsearch.index.query.FilterBuilders.andFilter;
import static org.elasticsearch.index.query.FilterBuilders.boolFilter;
import static org.elasticsearch.index.query.FilterBuilders.missingFilter;
import static org.elasticsearch.index.query.FilterBuilders.orFilter;
import static org.elasticsearch.index.query.FilterBuilders.rangeFilter;
import static org.elasticsearch.index.query.FilterBuilders.scriptFilter;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.FilterBuilders.termsFilter;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.joda.time.format.DateTimeFormat.forPattern;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 11/3/14.
 */
public class AdUnitProcessor {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Integer MAX_YEARS = 100;
    private static final String DEFAULT_CURRENCY = "USD";
    private static final float MIN_CTR_BOOST = 3.583519f;
    private static final float CPV_WEIGHT = 2.0f;

    private static final Integer SIZ_MULTIPLIER = 4;
    private static final Logger logger = getLogger(AdUnitProcessor.class);
    // JMX: com.netflix.servo.COUNTER.TotalAdsRequestsServed
    private static final Counter totalAdsRequestsServed = new BasicCounter(
            builder("TotalAdsRequestsServed").build());
    private static final DynamicStringProperty ctrScriptFunction =
            getInstance().getStringProperty("ctr.script.code", "");
    private static final DynamicStringProperty ctrScriptLang =
            getInstance().getStringProperty("ctr.script.lang", "expression");
    private static Client client;

    static {
        OBJECT_MAPPER.configure(FAIL_ON_UNKNOWN_PROPERTIES, false);
        DefaultMonitorRegistry.getInstance().register(totalAdsRequestsServed);
    }

    @Inject
    public AdUnitProcessor(Client esClient) {
        client = esClient;
    }

    public static List<AdUnitResponse> recommend(SearchQueryRequest sq, Integer positions) throws DeException {

        List<AdUnitResponse> adUnitResponses = null;
        if (sq != null) {
            DateTimeFormatter df = forPattern(getDateTimeFormatString());
            //If withOffsetParsed() is not used then everything is converted to local time
            //resulting in dayOfWeek and HourOfDay to be incorrect
            DateTime dt = df.withOffsetParsed().parseDateTime(sq.getTime());
            String timetable = dt.dayOfWeek().getAsText().toLowerCase() + ":" + dt.getHourOfDay() + ":" + "false";

            BoolFilterBuilder fb = boolFilter();
            fb.mustNot(termFilter("timetable", timetable));

            //do not target categories for widget format
            if (!equalsIgnoreCase(sq.getFormat(), INWIDGET.toString())) {
                fb.must(termsFilter("categories", toLowerCase(sq.getCategories())));
            }
            fb.must(termsFilter("languages", toLowerCase(sq.getLanguages())));
            fb.must(termsFilter("locations", toLowerCase(sq.getLocations())));
            fb.mustNot(termFilter("paused", true));
            fb.must(rangeFilter("start_date").lte(sq.getTime()));
            fb.must(rangeFilter("end_date").gte(sq.getTime()));

            if (isNotBlank(sq.getDevice())) {
                fb.must(termsFilter("devices", "all", sq.getDevice().toLowerCase()));
            } else {
                fb.must(termsFilter("devices", "all"));
            }
            if (isNotBlank(sq.getFormat())) {
                fb.must(termsFilter("formats", "all", sq.getFormat().toLowerCase()));
            } else {
                fb.must(termsFilter("formats", "all"));
            }
            if (!isEmptyList(sq.getLocations())) {
                fb.mustNot(termsFilter("excluded_locations", toLowerCase(sq.getLocations())));
            }
            //do not target categories for widget format
            if (!isEmptyList(sq.getCategories()) && !equalsIgnoreCase(sq.getFormat(), INWIDGET.toString())) {
                fb.mustNot(termsFilter("excluded_categories", toLowerCase(sq.getCategories())));
            }
            fb.must(orFilter(missingFilter("goal_views"),
                    missingFilter("views"),
                    scriptFilter("doc['views'].value < doc['goal_views'].value").lang("expression")));

            QueryBuilder qb = functionScoreQuery(fb)
                    .add(andFilter(rangeFilter("clicks").from(0), rangeFilter("impressions").from(0)),
                            scriptFunction(ctrScriptFunction.getValue()).lang(ctrScriptLang.getValue()))
                            //use a default boost equivalent to 100% ctr if adunit was created less than a day from now
//                    .add(orFilter(missingFilter("clicks"), missingFilter("impressions"),
//                            rangeFilter("_created").gt("now-1d")),
//                            ScoreFunctionBuilders.weightFactorFunction(MIN_CTR_BOOST))
//                            //use ctr function boost only if adunit was created more than a day ago
//                    .add(rangeFilter("_created").lte("now-1d"),
//                            scriptFunction(ctrScriptFunction.getValue()).lang(ctrScriptLang.getValue()))
                    .add(fieldValueFactorFunction("internal_cpv").setWeight(CPV_WEIGHT));

            List<String> excludedAds = sq.getExcludedVideoIds();
            if (!isEmptyList(excludedAds)) {
                for (String id : excludedAds) {
                    fb.mustNot(termsFilter("video_id", id));
                }
            }

            SearchRequestBuilder srb1 = client.prepareSearch(promotedIndex.get())
                    .setTypes(adunitsType.get())
                    .setSearchType(QUERY_AND_FETCH)
                    .setQuery(qb)
                    .setSize(positions * SIZ_MULTIPLIER);

            if (sq.isDebugEnabled()) {
                srb1.setExplain(true);
            }

            logger.info(srb1.toString());
            SearchResponse searchResponse;
            try {
                searchResponse = srb1.execute().actionGet();
            } catch (ElasticsearchException e) {
                throw new DeException(e, INTERNAL_SERVER_ERROR_500);
            }
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
                    throw new DeException(e, INTERNAL_SERVER_ERROR_500);
                }
            }
            adUnitResponses = removeDuplicateCampaigns(positions, adUnitResponses);
            logger.info("Num responses:" + adUnitResponses.size());

        }
        if (isEmptyList(adUnitResponses)) {
            //instead of returning no ads, return something even excluded-ad
            if (!isEmptyList(sq.getExcludedVideoIds())) {
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
        if (isBlank(cid)) {
            throw new DeException(new Throwable("no cid provided"), BAD_REQUEST_400);
        }
        BoolFilterBuilder fb = boolFilter().must(termFilter("campaign", cid));
        QueryBuilder qb = filteredQuery(null, fb);
        SearchRequestBuilder srb1 = client.prepareSearch(promotedIndex.get())
                .setTypes(adunitsType.get())
                .setSearchType(QUERY_AND_FETCH)
                .setQuery(qb);
        SearchResponse searchResponse = srb1.execute().actionGet();
        List<AdUnit> adUnits = new ArrayList<AdUnit>();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            AdUnit unit;
            try {
                unit = OBJECT_MAPPER.readValue(hit.getSourceAsString(), AdUnit.class);
            } catch (IOException e) {
                throw new DeException(e.getCause(), INTERNAL_SERVER_ERROR_500);
            }
            adUnits.add(unit);
        }
        if (isEmptyList(adUnits)) {
            return null;
        }
        return adUnits;
    }

    public static void insertAdUnit(AdUnit unit) throws DeException {
        if (unit == null) {
            throw new DeException(new Throwable("no adunit found in request body"), BAD_REQUEST_400);
        }
        updateAdUnit(modifyAdUnitForInsert(unit), true);
    }

    private static AdUnit modifyAdUnitForInsert(AdUnit unit) {
        if (isEmptyList(unit.getLocations())) {
            unit.setLocations(asList("all"));
        }
        if (isEmptyList(unit.getLanguages())) {
            unit.setLanguages(asList("all"));
        }

        if (isEmptyList(unit.getDevices())) {
            unit.setDevices(asList("all"));
        }
        if (isEmptyList(unit.getCategories())) {
            unit.setCategories(asList("all"));
        }
        if (isEmptyList(unit.getFormats())) {
            unit.setFormats(asList("all"));
        }

        List<String> sch = convertSchedulesToTimeTable(unit.getSchedules());

        if (isEmptyList(sch)) {
            unit.setTimetable(new ArrayList<String>(asList("all")));
        }
        unit.setTimetable(sch);
        unit.setSchedules(null);
        if (unit.getCpc() == null || unit.getCpc() == 0) {
            unit.setCpc(0L);
        }
        if (unit.getCpv() == null || unit.getCpv() == 0) {
            unit.setCpv(0L);
        }
        // if the internal vpc is not set, set it to the cpv
        if (unit.getInternaCpv() == null || unit.getCpv() == 0) {
            unit.setInternaCpv(unit.getCpv());
        }

        // if the currently not set, set it to dollar
        if (StringUtils.isBlank(unit.getCurrency())) {
            unit.setCurrency(DEFAULT_CURRENCY);
        }

        if (isBlank(unit.getStartDate())) {
            unit.setStartDate(timeToISO8601String(currentUTCTime()));
        }
        if (isBlank(unit.getEndDate())) {
            unit.setEndDate(timeToISO8601String(currentUTCTime().plusYears(MAX_YEARS)));
        }

        unit.setCategories(toLowerCase(unit.getCategories()));
        unit.setDevices(toLowerCase(unit.getDevices()));
        unit.setExcludedCategories(toLowerCase(unit.getExcludedCategories()));
        unit.setExcludedLocations(toLowerCase(unit.getExcludedLocations()));
        unit.setFormats(toLowerCase(unit.getFormats()));
        unit.setLanguages(toLowerCase(unit.getLanguages()));
        unit.setLocations(toLowerCase(unit.getLocations()));

        return unit;
    }

    public static void updateAdUnit(AdUnit unit, boolean enableUpsert) throws DeException {
        if (unit == null) {
            throw new DeException(new Throwable("no adunit found in request"), BAD_REQUEST_400);
        }
        try {
            client.prepareUpdate(promotedIndex.get(), adunitsType.get(), unit.getId())
                    .setDoc(OBJECT_MAPPER.writeValueAsString(unit))
                    .setDocAsUpsert(enableUpsert)
                    .setRetryOnConflict(retryOnConflictAdUnits.get())
                    .execute()
                    .actionGet();
        } catch (JsonProcessingException e) {
            throw new DeException(e, INTERNAL_SERVER_ERROR_500);
        } catch (ElasticsearchException e) {
            throw new DeException(e, INTERNAL_SERVER_ERROR_500);
        }
    }

    public static void insertAdUnitsInBulk(List<AdUnit> adUnits) throws DeException {
        if (isEmptyList(adUnits)) {
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
                bulkRequest.add(client.prepareUpdate(promotedIndex.get(), adunitsType.get(), adUnit.getId())
                        .setDoc(OBJECT_MAPPER.writeValueAsString(adUnit))
                        .setRetryOnConflict(retryOnConflictAdUnits.get())
                        .setDocAsUpsert(true));
            } catch (JsonProcessingException e) {
                logger.error("Error converting adunit to string", e);
                throw new DeException(e, INTERNAL_SERVER_ERROR_500);
            }
        }

        if (bulkRequest.numberOfActions() > 0) {
            BulkResponse bulkResponse;
            try {
                bulkResponse = bulkRequest.execute().actionGet();
            } catch (ElasticsearchException e) {
                throw new DeException(e, INTERNAL_SERVER_ERROR_500);
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
                throw new DeException(new Throwable("Error inserting adunits in Bulk"), INTERNAL_SERVER_ERROR_500);
            }
        }
    }

    public static AdUnit getAdUnitById(String id) throws DeException {
        if (isBlank(id)) {
            throw new DeException(new Throwable("id cannot be blank"), BAD_REQUEST_400);
        }
        GetResponse response = client.prepareGet(promotedIndex.get(), adunitsType.get(), id).execute().actionGet();
        AdUnit unit = null;
        byte[] responseSourceAsBytes = response.getSourceAsBytes();
        if (responseSourceAsBytes != null) {
            try {
                unit = OBJECT_MAPPER.readValue(responseSourceAsBytes, AdUnit.class);
            } catch (IOException e) {
                logger.error("error parsing adunit", e);
                throw new DeException(e, INTERNAL_SERVER_ERROR_500);
            }
        }
        return unit;
    }

    public static List<AdUnit> getAllAdUnits() throws DeException {
        QueryBuilder qb = matchAllQuery();

        //TODO fix this later - we should do paging instead of getting all docs
        SearchRequestBuilder srb1 = client.prepareSearch(promotedIndex.get())
                .setTypes(adunitsType.get())
                .setQuery(qb)
                .setSize(1000000);


        SearchResponse searchResponse = srb1.execute().actionGet();
        List<AdUnit> adUnits = new ArrayList<AdUnit>();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            AdUnit unit;
            try {
                unit = OBJECT_MAPPER.readValue(hit.getSourceAsString(), AdUnit.class);
            } catch (IOException e) {
                throw new DeException(e.getCause(), INTERNAL_SERVER_ERROR_500);
            }
            adUnits.add(unit);
        }
        if (isEmptyList(adUnits)) {
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
