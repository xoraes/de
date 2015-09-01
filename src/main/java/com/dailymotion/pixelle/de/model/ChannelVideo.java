package com.dailymotion.pixelle.de.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 3/4/15.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@Data
public class ChannelVideo {
    private static Logger LOGGER = getLogger(ChannelVideo.class);
    @JsonProperty("channel")
    private String channel;
    @JsonProperty("created_time")
    private Long createdTime;
    @JsonProperty("updated_time")
    private Long updatedTime;
    @JsonProperty("description")
    private String description;
    @JsonProperty("duration")
    private Integer duration;
    @JsonProperty("language")
    private String language;
    @JsonProperty("owner.id")
    private String ownerId;
    @JsonProperty("owner.username")
    private String ownerUsername;
    @JsonProperty("owner.screenname")
    private String ownerScreenName;
    @JsonProperty("thumbnail_url")
    private String thumbnailUrl;
    @JsonProperty("title")
    private String title;
    @JsonProperty("tags")
    private List<String> tags;
    @JsonProperty("3d")
    private Boolean ThreeDim;
    @JsonProperty("explicit")
    private Boolean explicit;
    @JsonProperty("ads")
    private Boolean ads;
    @JsonProperty("status")
    private String status;
    @JsonProperty("geoblocking")
    private List<String> geoBlocking;
    @JsonProperty("mediablocking")
    private List<Object> mediaBlocking;
    @JsonProperty("allow_embed")
    private Boolean allowEmbed;
    @JsonProperty("mode")
    private String mode;
    @JsonProperty("id")
    private String videoId;
}

