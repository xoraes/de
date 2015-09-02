package com.dailymotion.pixelle.de.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 12/10/14.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@Data
@EqualsAndHashCode(callSuper = false)
public class VideoResponse extends ItemsResponse {
    private static Logger logger = getLogger(VideoResponse.class);
    @JsonProperty("video_id")
    private String videoId;
    @JsonProperty("channel")
    private String channel;
    @JsonProperty("channel_name")
    private String channelName;
    @JsonProperty("channel_id")
    private String channelId;
    @JsonProperty("channel_tier")
    private String channelTier;
    @JsonProperty("description")
    private String description;
    @JsonProperty("title")
    private String title;
    @JsonProperty("duration")
    private Integer duration;
    @JsonProperty("resizable_thumbnail_url")
    private String resizableThumbnailUrl;
    @JsonProperty("type")
    private String contentType = "organic";
    @JsonProperty("debug")
    private String debugInfo;
}
