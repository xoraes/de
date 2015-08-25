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

        // get the min and max cpv given the location(s)
        List<String> locations = forecastRequest.getLocations();
        Long cpv = forecastRequest.getCpv();
        TermsFilterBuilder fb = null;
        if (!DeHelper.isEmptyList(locations)) {
            fb = FilterBuilders.termsFilter("locations", DeHelper.toLowerCase(locations));
        }
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

        float totalDailyOppCount = 1.0f, totalDailyViewCount = 1.0f;

        if (!DeHelper.isEmptyList(locations)) { // if locations is not provided then look at all countries
            // get the daily view and opp count based on location. add opp/view per location.
            for (String country : locations) {
                float dailyOppCount = 1.0f;
                dailyOppCount = CacheService.getCountryEventCountCache().get(country, "opportunity") / bqTimePeriod.get();
                // apply other filters (language/device/category/format)
                dailyOppCount = dailyOppCount * getFilteredPercentCountry(forecastRequest, country);

                totalDailyOppCount = totalDailyOppCount + dailyOppCount;
                totalDailyViewCount = totalDailyViewCount + (CacheService.getCountryEventCountCache().get(country, "view") / bqTimePeriod.get());

            }
        } else {
            totalDailyOppCount = CacheService.getCountryEventCountCache().get("total", "opportunity") / bqTimePeriod.get();
            // apply other filters (language/device/category/format)
            totalDailyOppCount = totalDailyOppCount * getFilteredPercentWithoutCountry(forecastRequest);
            totalDailyViewCount = CacheService.getCountryEventCountCache().get("total", "view") / bqTimePeriod.get();
        }

        Float dailyAvailableViews = totalDailyOppCount * vtr.get() - totalDailyViewCount;

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

    private static float getFilteredPercentCountry(ForecastRequest forecastRequest, String country) throws DeException {
        List<String> devices = forecastRequest.getDevices();
        List<String> formats = forecastRequest.getFormats();
        List<String> categories = forecastRequest.getCategories();
        List<String> languages = forecastRequest.getLanguages();
        float langPercent = 0.0f, numer = 1.0f, denom = 1.0f;
        if (!DeHelper.isEmptyList(languages)) {
            for (String lang : languages) {
                System.out.println(country);
                System.out.println(CacheService.getCountryLangCountCache());
                numer = CacheService.getCountryLangCountCache().get(country, lang);
                denom = CacheService.getCountryLangCountCache().get(country, "total");

                langPercent = langPercent + numer / denom;
            }
        }
        numer = 1.0f;
        denom = 1.0f;
        float devicePercent = 0.0f;
        if (!DeHelper.isEmptyList(devices)) {
            for (String device : devices) {
                numer = (float) CacheService.getCountryDeviceCountCache().get(country, device);
                denom = (float) CacheService.getCountryDeviceCountCache().get(country, "total");
                devicePercent = devicePercent + numer / denom;
            }
        }
        numer = 1.0f;
        denom = 1.0f;
        float formatPercent = 0.0f;
        if (!DeHelper.isEmptyList(formats)) {
            for (String format : formats) {
                numer = (float) CacheService.getCountryFormatCountCache().get(country, format);
                denom = (float) CacheService.getCountryFormatCountCache().get(country, "total");
                formatPercent = formatPercent + numer / denom;
            }
        }

        numer = 1.0f;
        denom = 1.0f;
        float categoryPercent = 0.0f;
        if (!DeHelper.isEmptyList(categories)) {
            for (String category : categories) {
                numer = (float) CacheService.getCountryCategoryCountCache().get(country, category);
                denom = (float) CacheService.getCountryCategoryCountCache().get(country, "total");
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

    private static float getFilteredPercentWithoutCountry(ForecastRequest forecastRequest) throws DeException {
        List<String> devices = forecastRequest.getDevices();
        List<String> formats = forecastRequest.getFormats();
        List<String> categories = forecastRequest.getCategories();
        List<String> languages = forecastRequest.getLanguages();
        float langPercent = 0.0f, numer = 1.0f, denom = 1.0f;
        if (!DeHelper.isEmptyList(languages)) {
            for (String lang : languages) {
                numer = (float) CacheService.getCountryLangCountCache().get("total", lang);
                denom = (float) CacheService.getCountryLangCountCache().get("total", "total");

                langPercent = langPercent + numer / denom;
            }
        }

        numer = 1.0f;
        denom = 1.0f;
        float devicePercent = 0.0f;
        if (!DeHelper.isEmptyList(devices)) {
            for (String device : devices) {
                numer = (float) CacheService.getCountryDeviceCountCache().get("total", device);
                denom = (float) CacheService.getCountryDeviceCountCache().get("total", "total");
                devicePercent = devicePercent + numer / denom;
            }
        }

        numer = 1.0f;
        denom = 1.0f;
        float formatPercent = 0.0f;
        if (!DeHelper.isEmptyList(formats)) {
            for (String format : formats) {
                numer = (float) CacheService.getCountryFormatCountCache().get("total", format);
                denom = (float) CacheService.getCountryFormatCountCache().get("total", "total");
                formatPercent = formatPercent + numer / denom;
            }
        }

        numer = 1.0f;
        denom = 1.0f;
        float categoryPercent = 0.0f;
        if (!DeHelper.isEmptyList(categories)) {
            for (String category : categories) {
                numer = (float) CacheService.getCountryCategoryCountCache().get("total", category);
                denom = (float) CacheService.getCountryCategoryCountCache().get("total", "total");
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