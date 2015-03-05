package com.dailymotion.pixelle.deserver.model;

import com.dailymotion.pixelle.deserver.processor.DeException;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by n.dhupia on 3/4/15.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ChannelVideo {
    private static Logger logger = LoggerFactory.getLogger(ChannelVideo.class);
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

    public ChannelVideo() {
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public Long getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(Long createdTime) {
        this.createdTime = createdTime;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Integer getDuration() {
        return duration;
    }

    public void setDuration(Integer duration) {
        this.duration = duration;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public String getOwnerUsername() {
        return ownerUsername;
    }

    public void setOwnerUsername(String ownerUsername) {
        this.ownerUsername = ownerUsername;
    }

    public String getThumbnailUrl() {
        return thumbnailUrl;
    }

    public void setThumbnailUrl(String thumbnailUrl) {
        this.thumbnailUrl = thumbnailUrl;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public Boolean getThreeDim() {
        return ThreeDim;
    }

    public void setThreeDim(Boolean threeDim) {
        ThreeDim = threeDim;
    }

    public Boolean getExplicit() {
        return explicit;
    }

    public void setExplicit(Boolean explicit) {
        this.explicit = explicit;
    }

    public Boolean getAds() {
        return ads;
    }

    public void setAds(Boolean ads) {
        this.ads = ads;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public List<String> getGeoBlocking() {
        return geoBlocking;
    }

    public void setGeoBlocking(List<String> geoBlocking) {
        this.geoBlocking = geoBlocking;
    }

    public List<Object> getMediaBlocking() {
        return mediaBlocking;
    }

    public void setMediaBlocking(List<Object> mediaBlocking) {
        this.mediaBlocking = mediaBlocking;
    }

    public Boolean getAllowEmbed() {
        return allowEmbed;
    }

    public void setAllowEmbed(Boolean allowEmbed) {
        this.allowEmbed = allowEmbed;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getVideoId() {
        return videoId;
    }

    public void setVideoId(String videoId) {
        this.videoId = videoId;
    }

    public String toString() {
        ObjectMapper mapper = new ObjectMapper();

        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            logger.error("error parsing json", e);
            throw new DeException(e, HttpStatus.INTERNAL_SERVER_ERROR_500);
        }
    }
}

