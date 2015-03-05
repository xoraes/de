package com.dailymotion.pixelle.deserver.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by n.dhupia on 3/3/15.
 */
public class ChannelVideos {
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private static Logger logger = LoggerFactory.getLogger(ChannelVideos.class);

    @JsonProperty("list")
    private List<ChannelVideo> list;

    public List<ChannelVideo> getList() {
        return list;
    }

    public void setList(List<ChannelVideo> list) {
        this.list = list;
    }
}
