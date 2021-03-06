package com.dailymotion.pixelle.de.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 10/29/14.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY) //this will ensure only non-null values are included in the serialized json
@Data
@EqualsAndHashCode(of = {"categories", "languages", "keywords"})
public class SearchQueryRequest {
    private static Logger logger = getLogger(SearchQueryRequest.class);
    @JsonProperty
    private boolean debugEnabled = false;
    @JsonProperty("positions")
    private Integer positions;
    @JsonProperty("type")
    private String allowedTypes;
    @JsonProperty("pattern")
    private String pattern;
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
    @JsonProperty("domain")
    private String domain;
    @JsonProperty("autoplay")
    private Boolean autoplay = false;
    @JsonProperty("browser")
    private String browser;
    @JsonProperty("playlist")
    private String playlist;
    @JsonProperty("keywords")
    private Map<String, Float> keywords;
    @JsonProperty("channels")
    private List<String> channels;
    @JsonProperty("sort")
    private String sortOrder;
    @JsonProperty("impression_history")
    private Map<String, Integer> impressionHistory;
    @JsonProperty("output")
    private String output;
    @JsonIgnore
    private String timeTable;
    @JsonIgnore
    private List<String> excludedVideoIds;
}

