package com.dailymotion.pixelle.deserver.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by n.dhupia on 8/12/15.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@Data
public class ForecastRequest {
    private static Logger logger = LoggerFactory.getLogger(ForecastRequest.class);
    @JsonProperty("languages")
    private List<String> languages;
    @JsonProperty("locations")
    private List<String> locations;
    @JsonProperty("formats")
    private List<String> formats;
    @JsonProperty("goal_period")
    private String goalPeriod;
    @JsonProperty("goal_views")
    private Float goalViews;
    @JsonProperty("duration")
    private Integer duration;
    @JsonProperty("devices")
    private List<String> devices;
    @JsonProperty("categories")
    private List<String> categories;
    @JsonProperty("cpv")
    private Long cpv;
    @JsonProperty("schedules")
    private Integer[] schedules;
    @JsonProperty("timetable")
    private List<String> timetable;
    @JsonProperty("start_date")
    private String startDate;
    @JsonProperty("end_date")
    private String endDate;
}