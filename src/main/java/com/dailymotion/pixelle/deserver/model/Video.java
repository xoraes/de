package com.dailymotion.pixelle.deserver.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@Data
public class Video {
    private static Logger logger = LoggerFactory.getLogger(Video.class);
    @JsonProperty("_id")
    private String id;
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
    @JsonProperty("tags")
    private List<String> tags;
    @JsonProperty("languages")
    private List<String> languages;
    @JsonProperty("description")
    private String description;
    @JsonProperty("title")
    private String title;
    @JsonProperty("duration")
    private Integer duration;
    @JsonProperty("categories")
    private List<String> categories;
    @JsonProperty("publication_date")
    private String publicationDate;
    @JsonProperty("_updated")
    private String updated;
    @JsonProperty("_created")
    private String created;
    @JsonProperty("resizable_thumbnail_url")
    private String resizableThumbnailUrl;
    @JsonProperty("clicks")
    private Integer clicks;
    @JsonProperty("views")
    private Integer views;
    @JsonProperty("impressions")
    private float impressions;
}
