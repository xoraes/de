package com.dailymotion.pixelle.deserver.model;

/**
 * Created by n.dhupia on 7/28/15.
 */

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by n.dhupia on 11/5/14.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@Data
public class ForecastResponse {
    private static Logger logger = LoggerFactory.getLogger(ForecastResponse.class);
    @JsonProperty("total_max_views")
    private Long totalMaxViews;
    @JsonProperty("total_min_views")
    private Long totalMinViews;
    @JsonProperty("daily_max_views")
    private Long dailyMaxViews;
    @JsonProperty("daily_min_views")
    private Long dailyMinViews;
}
