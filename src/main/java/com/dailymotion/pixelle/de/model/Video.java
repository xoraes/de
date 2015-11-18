package com.dailymotion.pixelle.de.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.slf4j.Logger;

import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@Data
public class Video {
    private static Logger logger = getLogger(Video.class);
    @JsonProperty("_id")
    private String id;
    @JsonProperty("channel_name")
    private String channelName;
    @JsonProperty("channel_id")
    private String channelId;
    @JsonProperty("channel_tier")
    private String channelTier;
    @JsonProperty("channel")
    private String channel;
    @JsonProperty("languages")
    private List<String> languages;
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
    @JsonProperty("video_id")
    private String videoId;
    @JsonProperty("id")
    private String videoIdFromDM;

}
