package com.dailymotion.pixelle.forecast.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.slf4j.Logger;

import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 8/12/15.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@Data
public class ForecastRequest {
    private static Logger logger = getLogger(ForecastRequest.class);
    @JsonProperty("languages")
    private List<String> languages;
    @JsonProperty("locations")
    private List<String> locations;
    @JsonProperty("formats")
    private List<String> formats;
    @JsonProperty("duration")
    private Integer duration;
    @JsonProperty("devices")
    private List<String> devices;
    @JsonProperty("categories")
    private List<String> categories;
    @JsonProperty("cpv")
    private Integer cpv;
    @JsonProperty("schedules")
    private Integer[] schedules;
    @JsonProperty("timetable")
    private List<String> timetable;
    @JsonProperty("start_date")
    private String startDate;
    @JsonProperty("end_date")
    private String endDate;
}
