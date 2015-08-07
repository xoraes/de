package com.dailymotion.pixelle.deserver.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by n.dhupia on 10/29/14.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY) //this will ensure only non-null values are included in the serialized json
@Data
@EqualsAndHashCode(of = {"categories", "languages"})
public class SearchQueryRequest {
    private static Logger logger = LoggerFactory.getLogger(SearchQueryRequest.class);
    @JsonProperty("languages")
    private List<String> languages;
    @JsonProperty("locations")
    private List<String> locations;
    @JsonProperty("categories")
    private List<String> categories;
    @JsonProperty("device")
    private String device;
    @JsonProperty("format")
    private String format;
    @JsonProperty("time")
    private String time;
    @JsonProperty("browser")
    private String browser;
    @JsonProperty("channels")
    private List<String> channels; //comma separated list of channels
    @JsonProperty("sort")
    private String sortOrder;
    @JsonProperty("impression_history")
    private Map<String, Integer> impressionHistory;
    @JsonIgnore
    private String timeTable;
    @JsonIgnore
    private boolean debugEnabled;
    @JsonIgnore
    private List<String> excludedVideoIds;
}

