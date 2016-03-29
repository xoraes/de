package com.dailymotion.pixelle.de.model;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 10/30/14.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY) //this will ensure only non-null values are included in the serialized json
@Data
@EqualsAndHashCode(callSuper = false)
public class AdUnitResponse extends ItemsResponse {
    private static Logger logger = getLogger(AdUnitResponse.class);
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
    @JsonProperty("account")
    private String accountId;
    @JsonProperty("video_id")
    private String videoId;
    @JsonProperty("description")
    private String description;
    @JsonProperty("title")
    private String title;
    @JsonProperty("duration")
    private Integer duration;
    @JsonProperty("cpc")
    private Long cpc;
    @JsonProperty("cpv")
    private Long cpv;
    @JsonProperty("internal_cpv")
    private Long internalCpv;
    @JsonProperty("currency")
    private String currency;
    @JsonProperty("autoplay")
    private Boolean autoplay = false;
    @JsonProperty("resizable_thumbnail_url")
    private String resizableThumbnailUrl;
    @JsonProperty("type")
    private String contentType = "promoted";
    @JsonProperty("custom_video_url")
    private String customVideoUrl;
    @JsonProperty("debug")
    private String debugInfo;
}