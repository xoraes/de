package com.dailymotion.pixelle.forecast.processor;

import com.dailymotion.pixelle.forecast.model.ForecastRequest;
import com.dailymotion.pixelle.forecast.model.ForecastResponse;
import com.dailymotion.pixelle.forecast.model.ForecastViews;
import com.google.inject.Inject;
import com.netflix.config.DynamicFloatProperty;
import com.netflix.config.DynamicIntProperty;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.TermsFilterBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static com.dailymotion.pixelle.common.services.CacheService.getCountryCategoryCountCache;
import static com.dailymotion.pixelle.common.services.CacheService.getCountryDeviceCountCache;
import static com.dailymotion.pixelle.common.services.CacheService.getCountryEventCountCache;
import static com.dailymotion.pixelle.common.services.CacheService.getCountryFormatCountCache;
import static com.dailymotion.pixelle.common.services.CacheService.getCountryLangCountCache;
import static com.dailymotion.pixelle.de.processor.DeHelper.adunitsType;
import static com.dailymotion.pixelle.de.processor.DeHelper.getDateTimeFormatString;
import static com.dailymotion.pixelle.de.processor.DeHelper.isEmptyList;
import static com.dailymotion.pixelle.de.processor.DeHelper.promotedIndex;
import static com.google.api.client.repackaged.com.google.common.base.Objects.firstNonNull;
import static com.netflix.config.DynamicPropertyFactory.getInstance;
import static java.lang.Double.valueOf;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.lowerCase;
import static org.eclipse.jetty.http.HttpStatus.BAD_REQUEST_400;
import static org.elasticsearch.action.search.SearchType.QUERY_AND_FETCH;
import static org.elasticsearch.index.query.FilterBuilders.termsFilter;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.min;
import static org.joda.time.Days.daysBetween;
import static org.joda.time.format.DateTimeFormat.forPattern;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 8/12/15.
 */
public class Forecaster {
    private static final DynamicFloatProperty VTR =
            getInstance().getFloatProperty("pixelle.forecast.vtr", 0.0025f);
    private static final DynamicIntProperty MAX_CPV =
            getInstance().getIntProperty("pixelle.forecast.cpv.max", 100);
    private static final DynamicIntProperty MIN_CPV =
            getInstance().getIntProperty("pixelle.forecast.cpv.min", 1);

    private static final Float HOUSRSINWEEK = 168.0f;
    private static final String TOTAL = "total";
    private static final float BQ_TIMEPERIOD = 21.0f;

    private static final Logger LOGGER = getLogger(Forecaster.class);
    static Client client;

    @Inject
    public Forecaster(Client esClient) {
        client = esClient;
    }

    public static ForecastResponse forecast(ForecastRequest forecastRequest) throws ForecastException {

        if (forecastRequest == null) {
            throw new ForecastException(BAD_REQUEST_400, "Cpv and country code must be provided");
        }
        if (forecastRequest.getCpv() == null || forecastRequest.getCpv() < 1) {
            throw new ForecastException(BAD_REQUEST_400, "Cpv must be provided and be greater or equal to 1");
        }

        Integer cpv = forecastRequest.getCpv();

        // get the min and max cpv given the location(s)
        TermsFilterBuilder fb = null;

        List<String> locations = forecastRequest.getLocations();
        if (!isEmptyList(locations)) {
            locations.add("all");
            fb = termsFilter("locations", locations);
        }
        FilteredQueryBuilder qb = filteredQuery(null, fb);
        SearchRequestBuilder srb1 = client.prepareSearch(promotedIndex.get())
                .setTypes(adunitsType.get())
                .setSearchType(QUERY_AND_FETCH)
                .setQuery(qb)
                .addAggregation(max("max").field("cpv")).addAggregation(min("min").field("cpv"));

        LOGGER.info(srb1.toString());
        SearchResponse searchResponse = srb1.execute().actionGet();

        LOGGER.info(searchResponse.toString());
        Aggregation minAggs = searchResponse.getAggregations().get("min");
        Aggregation maxAggs = searchResponse.getAggregations().get("max");

        Min min = (Min) minAggs;
        Integer minCpvValue = 1, maxCpvValue = cpv;
        if (!valueOf(min.getValue()).isInfinite()) {
            minCpvValue = valueOf(min.getValue()).intValue();
        }
        Max max = (Max) maxAggs;
        if (!valueOf(max.getValue()).isInfinite()) {
            maxCpvValue = valueOf(max.getValue()).intValue();
        }
        if (maxCpvValue > MAX_CPV.get()) {
            maxCpvValue = MAX_CPV.get();
        }

        List<ForecastViews> forecastViewList = new ArrayList<>();
        for (int i = cpv; i <= maxCpvValue; i++) {
            ForecastViews forecastViews = forecastViews(i, maxCpvValue, minCpvValue, forecastRequest);
            forecastViewList.add(forecastViews);
        }
        ForecastResponse response = new ForecastResponse();
        response.setForecastViewsList(forecastViewList);
        return response;
    }


    public static ForecastViews forecastViews(int cpv, Integer maxCpvValue, Integer minCpvValue, ForecastRequest forecastRequest) throws ForecastException {
        List<String> locations = forecastRequest.getLocations();

        float totalDailyOppCount = 1.0f, totalDailyViewCount = 1.0f;

        if (!isEmptyList(locations)) { // if locations is not provided then look at all countries
            // get the daily view and opp count based on location. add opp/view per location.
            for (String country : locations) {
                country = lowerCase(country);
                if (StringUtils.equals(country, "all")) {
                    continue;
                }
                float dailyOppCount = 1.0f, dailyViewCount = 1.0f;
                dailyOppCount = firstNonNull(getCountryEventCountCache().get(country, "opportunity"), 0L) / BQ_TIMEPERIOD;
                // apply other filters (language/device/category/format)
                dailyOppCount = dailyOppCount * getFilteredPercentCountry(forecastRequest, country);
                totalDailyOppCount = totalDailyOppCount + dailyOppCount;

                dailyViewCount = firstNonNull(getCountryEventCountCache().get(country, "view"), 0L) / BQ_TIMEPERIOD;
                totalDailyViewCount = totalDailyViewCount + dailyViewCount;

            }
        } else {
            totalDailyOppCount = firstNonNull(getCountryEventCountCache().get(TOTAL, "opportunity"), 0l) / BQ_TIMEPERIOD;
            // apply other filters (language/device/category/format)
            totalDailyOppCount = totalDailyOppCount * getFilteredPercentWithoutCountry(forecastRequest);
            totalDailyViewCount = firstNonNull(getCountryEventCountCache().get(TOTAL, "view"), 0L) / BQ_TIMEPERIOD;
        }


        float dailyAvailableViews = totalDailyOppCount * VTR.get();

        float ratio = 1.0f;
        Integer diffCpv = maxCpvValue - minCpvValue;
        if (cpv >= maxCpvValue) {
            ratio = 1.0f;
        } else if (cpv <= minCpvValue || diffCpv < 1) { // we don't want num or denominator to be zero
            ratio = cpv / maxCpvValue;
        } else {
            ratio = ((float) cpv - minCpvValue) / diffCpv;
        }

        Long dailyMaxViews = (long) (dailyAvailableViews * ratio - totalDailyViewCount * (1 - ratio));
        Long dailyMinViews = (long) (dailyMaxViews * 0.25f);
        Long dailyAvgViews = (long) (dailyMaxViews * 0.5f);


        if (dailyMaxViews <= 1) {
            dailyMaxViews = 100L;
        }

        if (dailyMinViews <= 1) {
            dailyMinViews = 10L;
        }

        if (dailyAvgViews <= 1) {
            dailyAvgViews = 50L;
        }
        ForecastViews forecastViews = new ForecastViews();
        forecastViews.setCpv(cpv);

        forecastViews.setDailyMaxViews(dailyMaxViews);
        forecastViews.setDailyMinViews(dailyMinViews);
        forecastViews.setDailyAvgViews(dailyAvgViews);


        // calculate total
        float avgHours = getHoursPerWeekFromSchedule(forecastRequest.getSchedules()) / HOUSRSINWEEK;
        Integer numDays = getDaysInBetween(forecastRequest.getStartDate(), forecastRequest.getEndDate());

        Long totalMaxValues, totalMinValues, totalAvgValues;

        //return total views only if schedules and start/end date is present
        if (avgHours > 0 && numDays > 0) {
            totalMaxValues = (long) (dailyMaxViews * avgHours * numDays);
            totalMinValues = (long) (dailyMinViews * avgHours * numDays);
            totalAvgValues = (long) (dailyAvgViews * avgHours * numDays);
            if (totalMaxValues > 0) {
                forecastViews.setTotalMaxViews(totalMaxValues);
                forecastViews.setTotalMinViews(totalMinValues);
                forecastViews.setTotalAvgViews(totalAvgValues);
            }
        }
        return forecastViews;

    }

    private static Boolean isHourSet(int hour, int mask) {
        return (mask & (1 << hour)) > 0;
    }

    private static int getHoursPerWeekFromSchedule(Integer[] schedules) {

        if (schedules == null || schedules.length < 7) {
            return HOUSRSINWEEK.intValue(); // total number of hours in week
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
        if (isBlank(startDate) || isBlank(endDate)) {
            return 0;
        }
        DateTimeFormatter formatter = forPattern(getDateTimeFormatString());
        DateTime sDate = formatter.parseDateTime(startDate);
        DateTime eDate = formatter.parseDateTime(endDate);
        return daysBetween(sDate.toLocalDate(), eDate.toLocalDate()).getDays();
    }

    private static float getFilteredPercentCountry(ForecastRequest forecastRequest, String country) throws ForecastException {
        List<String> devices = forecastRequest.getDevices();
        List<String> formats = forecastRequest.getFormats();
        List<String> categories = forecastRequest.getCategories();
        List<String> languages = forecastRequest.getLanguages();
        float langPercent = 0.0f, numer = 1.0f, denom = 1.0f;
        if (!isEmptyList(languages)) {
            for (String lang : languages) {
                numer = firstNonNull(getCountryLangCountCache().get(country, lang), 0L);
                denom = firstNonNull(getCountryLangCountCache().get(country, TOTAL), 1L);
                langPercent = langPercent + numer / denom;
            }
        }
        numer = 1.0f;
        denom = 1.0f;
        float devicePercent = 0.0f;
        if (!isEmptyList(devices)) {
            for (String device : devices) {
                numer = firstNonNull(getCountryDeviceCountCache().get(country, device), 0L);
                denom = firstNonNull(getCountryDeviceCountCache().get(country, TOTAL), 1L);
                devicePercent = devicePercent + numer / denom;
            }
        }
        numer = 1.0f;
        denom = 1.0f;
        float formatPercent = 0.0f;
        if (!isEmptyList(formats)) {
            for (String format : formats) {
                numer = firstNonNull(getCountryFormatCountCache().get(country, format), 0L);
                denom = firstNonNull(getCountryFormatCountCache().get(country, TOTAL), 1L);
                formatPercent = formatPercent + numer / denom;
            }
        }

        numer = 1.0f;
        denom = 1.0f;
        float categoryPercent = 0.0f;
        if (!isEmptyList(categories)) {
            for (String category : categories) {
                numer = firstNonNull(getCountryCategoryCountCache().get(country, category), 0L);
                denom = firstNonNull(getCountryCategoryCountCache().get(country, TOTAL), 1L);
                categoryPercent = categoryPercent + numer / denom;
            }
        }

        if (langPercent <= 0.0f) {
            langPercent = 1.0f;
        }
        if (devicePercent <= 0.0f) {
            devicePercent = 1.0f;
        }
        if (categoryPercent <= 0.0f) {
            categoryPercent = 1.0f;
        }
        if (formatPercent <= 0.0f) {
            formatPercent = 1.0f;
        }

        return langPercent * categoryPercent * formatPercent * devicePercent;
    }

    private static float getFilteredPercentWithoutCountry(ForecastRequest forecastRequest) throws ForecastException {
        List<String> devices = forecastRequest.getDevices();
        List<String> formats = forecastRequest.getFormats();
        List<String> categories = forecastRequest.getCategories();
        List<String> languages = forecastRequest.getLanguages();
        float langPercent = 0.0f, numer = 1.0f, denom = 1.0f;
        if (!isEmptyList(languages)) {
            for (String lang : languages) {
                numer = firstNonNull(getCountryLangCountCache().get(TOTAL, lang), 0L);
                denom = firstNonNull(getCountryLangCountCache().get(TOTAL, TOTAL), 1L);

                langPercent = langPercent + numer / denom;
            }
        }

        numer = 1.0f;
        denom = 1.0f;
        float devicePercent = 0.0f;
        if (!isEmptyList(devices)) {
            for (String device : devices) {
                numer = firstNonNull(getCountryDeviceCountCache().get(TOTAL, device), 0L);
                denom = firstNonNull(getCountryDeviceCountCache().get(TOTAL, TOTAL), 1L);
                devicePercent = devicePercent + numer / denom;
            }
        }

        numer = 1.0f;
        denom = 1.0f;
        float formatPercent = 0.0f;
        if (!isEmptyList(formats)) {
            for (String format : formats) {
                numer = firstNonNull(getCountryFormatCountCache().get(TOTAL, format), 0L);
                denom = firstNonNull(getCountryFormatCountCache().get(TOTAL, TOTAL), 1L);
                formatPercent = formatPercent + numer / denom;
            }
        }

        numer = 1.0f;
        denom = 1.0f;
        float categoryPercent = 0.0f;
        if (!isEmptyList(categories)) {
            for (String category : categories) {
                numer = firstNonNull(getCountryCategoryCountCache().get(TOTAL, category), 0L);
                denom = firstNonNull(getCountryCategoryCountCache().get(TOTAL, TOTAL), 1L);
                categoryPercent = categoryPercent + numer / denom;
            }
        }

        if (langPercent <= 0.0f) {
            langPercent = 1.0f;
        }
        if (devicePercent <= 0.0f) {
            devicePercent = 1.0f;
        }
        if (categoryPercent <= 0.0f) {
            categoryPercent = 1.0f;
        }
        if (formatPercent <= 0.0f) {
            formatPercent = 1.0f;
        }
        return langPercent * categoryPercent * formatPercent * devicePercent;
    }
}