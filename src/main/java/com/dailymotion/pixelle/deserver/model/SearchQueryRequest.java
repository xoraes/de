package com.dailymotion.pixelle.deserver.model;

import com.dailymotion.pixelle.deserver.logger.InjectLogger;
import com.dailymotion.pixelle.deserver.processor.DeException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;

import java.util.List;

/**
 * Created by n.dhupia on 10/29/14.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY) //this will ensure only non-null values are included in the serialized json
public class SearchQueryRequest {
    @InjectLogger
    private static Logger logger;

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
    @JsonIgnore
    private String timeTable;

    public SearchQueryRequest() {
    }

    public List<String> getLanguages() {
        return languages;
    }

    public void setLanguages(List<String> languages) {
        this.languages = languages;
    }

    public List<String> getLocations() {
        return locations;
    }

    public void setLocations(List<String> locations) {
        this.locations = locations;
    }

    public List<String> getCategories() {
        return categories;
    }

    public void setCategories(List<String> categories) {
        this.categories = categories;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getTimeTable() {
        return timeTable;
    }

    public void setTimeTable(String timeTable) {
        this.timeTable = timeTable;
    }

    public String toString() {
        ObjectMapper mapper = new ObjectMapper();

        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            logger.error("error parsing json", e);
            throw new DeException(e, 500);
        }
    }
}
