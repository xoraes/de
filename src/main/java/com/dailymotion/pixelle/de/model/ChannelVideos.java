package com.dailymotion.pixelle.de.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.slf4j.Logger;

import java.util.List;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 3/3/15.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(NON_EMPTY)
@Data
public class ChannelVideos {
    private static Logger logger = getLogger(ChannelVideos.class);
    @JsonProperty("list")
    private List<ChannelVideo> list;
}
