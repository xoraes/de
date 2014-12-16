package com.dailymotion.pixelle.deserver.model;


import com.dailymotion.pixelle.deserver.processor.DeException;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by n.dhupia on 10/30/14.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY) //this will ensure only non-null values are included in the serialized json
public class AdUnitResponse extends ItemsResponse {
    private static Logger logger = LoggerFactory.getLogger(AdUnitResponse.class);
    @JsonProperty("ad")
    private String ad;
    @JsonProperty("campaign")
    private String campaignId;
    @JsonProperty("tactic")
    private String tacticId;
    @JsonProperty("channel")
    private String channel;
    @JsonProperty("channel_name")
    private String channelName;
    @JsonProperty("channel_id")
    private String channelId;
    @JsonProperty("channel_url")
    private String channelUrl;
    @JsonProperty("account")
    private String accountId;
    @JsonProperty("video_id")
    private String videoId;
    @JsonProperty("thumbnail_url")
    private String thumbnailUrl;
    @JsonProperty("description")
    private String description;
    @JsonProperty("title")
    private String title;
    @JsonProperty("duration")
    private Integer duration;
    @JsonProperty("cpc")
    private Long cpc;
    @JsonProperty("resizable_thumbnail_url")
    private String resizableThumbnailUrl;
    @JsonProperty("type")
    private String contentType = "promoted";


    public String getAd() {
        return ad;
    }

    public void setAd(String ad) {
        this.ad = ad;
    }

    public String getCampaignId() {
        return campaignId;
    }

    public void setCampaignId(String campaignId) {
        this.campaignId = campaignId;
    }

    public String getTacticId() {
        return tacticId;
    }

    public void setTacticId(String tacticId) {
        this.tacticId = tacticId;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getChannelName() {
        return channelName;
    }

    public void setChannelName(String channelName) {
        this.channelName = channelName;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public String getChannelUrl() {
        return channelUrl;
    }

    public void setChannelUrl(String channelUrl) {
        this.channelUrl = channelUrl;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getVideoId() {
        return videoId;
    }

    public void setVideoId(String videoId) {
        this.videoId = videoId;
    }

    public String getThumbnailUrl() {
        return thumbnailUrl;
    }

    public void setThumbnailUrl(String thumbnailUrl) {
        this.thumbnailUrl = thumbnailUrl;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Integer getDuration() {
        return duration;
    }

    public void setDuration(Integer duration) {
        this.duration = duration;
    }

    public Long getCpc() {
        return cpc;
    }

    public void setCpc(Long cpc) {
        this.cpc = cpc;
    }

    public String getResizableThumbnailUrl() {
        return resizableThumbnailUrl;
    }

    public void setResizableThumbnailUrl(String resizableThumbnailUrl) {
        this.resizableThumbnailUrl = resizableThumbnailUrl;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
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
