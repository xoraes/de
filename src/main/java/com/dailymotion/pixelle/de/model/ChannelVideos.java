package com.dailymotion.pixelle.de.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.fasterxml.jackson.annotation.JsonInclude.Include;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 3/3/15.
 */
public class ChannelVideos {
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(NON_EMPTY)
    private static Logger LOGGER = getLogger(ChannelVideos.class);

    @JsonProperty("list")
    private List<ChannelVideo> list;

    public List<ChannelVideo> getList() {
        return list;
    }

    public void setList(List<ChannelVideo> list) {
        this.list = list;
    }
}
