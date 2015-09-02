package com.dailymotion.pixelle.forecast.model;

/**
 * Created by n.dhupia on 7/28/15.
 */

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 11/5/14.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@Data
public class ForecastViews {
    private static Logger logger = getLogger(ForecastViews.class);
    @JsonProperty("total_avg")
    private Long totalAvgViews;
    @JsonProperty("total_max")
    private Long totalMaxViews;
    @JsonProperty("total_min")
    private Long totalMinViews;
    @JsonProperty("daily_max")
    private Long dailyMaxViews;
    @JsonProperty("daily_min")
    private Long dailyMinViews;
    @JsonProperty("daily_avg")
    private Long dailyAvgViews;
    @JsonProperty("cpv")
    private Integer cpv;


}
