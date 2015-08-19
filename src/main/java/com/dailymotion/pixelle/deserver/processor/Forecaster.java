package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.ForecastRequest;
import com.dailymotion.pixelle.deserver.model.ForecastResponse;
import com.dailymotion.pixelle.deserver.processor.service.CacheService;
import com.google.inject.Inject;
import com.netflix.config.DynamicFloatProperty;
import com.netflix.config.DynamicPropertyFactory;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.http.HttpStatus;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsFilterBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by n.dhupia on 8/12/15.
 */
public class Forecaster {
    private static final DynamicFloatProperty vtr =
            DynamicPropertyFactory.getInstance().getFloatProperty("pixelle.vtr", 0.0025f);
    private static final DynamicFloatProperty bqTimePeriod =
            DynamicPropertyFactory.getInstance().getFloatProperty("bq.eventdata.timelapse", 21.0f);

    private static final Logger logger = LoggerFactory.getLogger(Forecaster.class);
    static Client client;

    @Inject
    public Forecaster(Client esClient) {
        client = esClient;
    }

    public static ForecastResponse forecast(ForecastRequest forecastRequest) throws DeException {

        if (forecastRequest == null) {
            throw new DeException(HttpStatus.BAD_REQUEST_400, "Cpv and country code must be provided");
        }
        if (forecastRequest.getCpv() == null || forecastRequest.getCpv() < 1) {
            throw new DeException(HttpStatus.BAD_REQUEST_400, "Cpv must be provided and be greater or equal to 1");
        }
        if (DeHelper.isEmptyList(forecastRequest.getLocations())) {
            throw new DeException(HttpStatus.BAD_REQUEST_400, "Location list must be provided");
        }
        // get the min and max cpv given the location(s)
        List<String> locations = forecastRequest.getLocations();
        Long cpv = forecastRequest.getCpv();
        TermsFilterBuilder fb = FilterBuilders.termsFilter("locations", DeHelper.toLowerCase(locations));
        FilteredQueryBuilder qb = QueryBuilders.filteredQuery(null, fb);
        SearchRequestBuilder srb1 = client.prepareSearch(DeHelper.promotedIndex.get())
                .setTypes(DeHelper.adunitsType.get())
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(qb)
                .addAggregation(AggregationBuilders.max("max").field("cpv")).addAggregation(AggregationBuilders.min("min").field("cpv"));

        logger.info(srb1.toString());
        SearchResponse searchResponse = srb1.execute().actionGet();

        logger.info(searchResponse.toString());
        Aggregation minAggs = searchResponse.getAggregations().get("min");
        Min min = (Min) minAggs;
        float minCpvValue = (float) min.getValue();

        Aggregation maxAggs = searchResponse.getAggregations().get("max");
        Max max = (Max) maxAggs;
        float maxCpvValue = (float) max.getValue();

        // make sure min and max cpv >= 1
        if (minCpvValue == 0) {
            minCpvValue = 1;
        }

        if (maxCpvValue == 0) {
            maxCpvValue = 1;
        }

        float dailyOppCount = 1.0f, dailyViewCount = 1.0f;

        // get the daily view and opp count based on location. add opp/view per location.
        for (String country : locations) {
            try {
                dailyOppCount = (dailyOppCount + CacheService.getEventCountCache().get("opportunity").get(country.toUpperCase())) / bqTimePeriod.get();

            } catch (ExecutionException e) {
                logger.error("Error getting opportunity count from cache for country {}", country);
            }
            try {
                dailyViewCount = (dailyViewCount + CacheService.getEventCountCache().get("view").get(country.toUpperCase())) / bqTimePeriod.get();
            } catch (ExecutionException e) {
                logger.error("Error getting opportunity count from cache for country {}", country);
            }
        }

        Float dailyAvailableViews = dailyOppCount * vtr.get() - dailyViewCount;

        dailyAvailableViews = applyStaticFilterRules(dailyAvailableViews, forecastRequest);

        float ratio = 1.0f;
        Float diffCpv = maxCpvValue - minCpvValue;
        if (cpv >= maxCpvValue) {
            ratio = 1.0f;
        } else if (cpv <= minCpvValue || diffCpv < 1) { // we don't want num or denominator to be zero
            ratio = cpv / maxCpvValue;
        } else {
            ratio = ((float) cpv - minCpvValue) / diffCpv;
        }

        Long dailyMaxViews = Float.valueOf((dailyAvailableViews * 1.0f) * ratio).longValue();
        Long dailyMinViews = Float.valueOf((dailyAvailableViews * 0.25f) * ratio).longValue();

        if (dailyMaxViews <= 1) {
            dailyMaxViews = 100L;
        }

        if (dailyMinViews <= 1) {
            dailyMinViews = 50L;
        }
        ForecastResponse response = new ForecastResponse();
        response.setDailyMaxViews(dailyMaxViews);
        response.setDailyMinViews(dailyMinViews);


        // calculate total
        float avgHours = getHoursPerWeekFromSchedule(forecastRequest.getSchedules()) / 168.0f;
        Integer numDays = getDaysInBetween(forecastRequest.getStartDate(), forecastRequest.getEndDate());

        Long totalMaxValues, totalMinValues;

        //return total views only if schedules and start/end date is present
        if (avgHours > 0 && numDays > 0) {
            totalMaxValues = Float.valueOf(dailyMaxViews * avgHours * numDays).longValue();
            totalMinValues = Float.valueOf(dailyMinViews * avgHours * numDays).longValue();
            if (totalMaxValues > 0) {
                response.setTotalMaxViews(totalMaxValues);
                response.setTotalMinViews(totalMinValues);
            }
        }
        return response;
    }

    private static Boolean isHourSet(int hour, int mask) {
        return (mask & (1 << hour)) > 0;
    }

    private static int getHoursPerWeekFromSchedule(Integer[] schedules) {

        if (schedules == null || schedules.length < 7) {
            return 168; // total number of hours in week
        }

        Boolean hourSet;
        int weeklyHours = 0;

        for (int i = 0; i < 7; i++) {
            for (int j = 0; j < 24; j++) {
                hourSet = isHourSet(j, schedules[i]);
                if (hourSet) {
                    weeklyHours++;
                }
            }
        }
        return weeklyHours;
    }

    private static int getDaysInBetween(String startDate, String endDate) {
        if (StringUtils.isBlank(startDate) || StringUtils.isBlank(endDate)) {
            return 0;
        }
        DateTimeFormatter formatter = DateTimeFormat.forPattern(DeHelper.getDateTimeFormatString());
        DateTime sDate = formatter.parseDateTime(startDate);
        DateTime eDate = formatter.parseDateTime(endDate);
        return Days.daysBetween(sDate.toLocalDate(), eDate.toLocalDate()).getDays();
    }

    private static float applyStaticFilterRules(float dailyAvailableViews, ForecastRequest forecastRequest) {
        List<String> devices = forecastRequest.getDevices();
        List<String> formats = forecastRequest.getFormats();
        List<String> categories = forecastRequest.getCategories();

        if (!DeHelper.isEmptyList(devices)) {
            if (!devices.contains("desktop")) {
                dailyAvailableViews = dailyAvailableViews - dailyAvailableViews * 0.45f;
            }
            if (!devices.contains("mobile")) {
                dailyAvailableViews = dailyAvailableViews - dailyAvailableViews * 0.45f;
            }
            if (!devices.contains("tablet")) {
                dailyAvailableViews = dailyAvailableViews - dailyAvailableViews * 0.09f;
            }
            if (!devices.contains("tv")) {
                dailyAvailableViews = dailyAvailableViews - dailyAvailableViews * 0.01f;
            }
        }

        if (!DeHelper.isEmptyList(formats)) {
            if (!devices.contains("in-feed")) {
                dailyAvailableViews = dailyAvailableViews - dailyAvailableViews * 0.15f;
            }
            if (!devices.contains("in-search")) {
                dailyAvailableViews = dailyAvailableViews - dailyAvailableViews * 0.10f;
            }
            if (!devices.contains("in-related")) {
                dailyAvailableViews = dailyAvailableViews - dailyAvailableViews * 0.60f;
            }
            if (!devices.contains("in-widget")) {
                dailyAvailableViews = dailyAvailableViews - dailyAvailableViews * 0.15f;
            }
        }

        if (!DeHelper.isEmptyList(categories)) {
            int numCategories = categories.size();
            dailyAvailableViews = dailyAvailableViews * numCategories / 16.0f;
        }
        return dailyAvailableViews;
    }
}

