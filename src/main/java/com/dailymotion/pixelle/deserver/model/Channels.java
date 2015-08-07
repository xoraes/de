package com.dailymotion.pixelle.deserver.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

/**
 * Created by n.dhupia on 8/6/15.
 * This class object will serve as the key to the cache containing Channels(key) and ListOfVideos(Values)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@Data
public class Channels {
    private String channels;
    private String sortOrder = "recent";

    public Channels(String channels, String sortOrder) {
        this.channels = channels;
        this.sortOrder = sortOrder;
    }
}
