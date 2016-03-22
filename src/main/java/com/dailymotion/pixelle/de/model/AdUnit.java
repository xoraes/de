package com.dailymotion.pixelle.de.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.slf4j.Logger;

import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 11/5/14.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@Data
public class AdUnit {
    private static Logger logger = getLogger(AdUnit.class);
    @JsonProperty("_id")
    private String id;
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
    @JsonProperty("languages")
    private List<String> languages;
    @JsonProperty("locations")
    private List<String> locations;
    @JsonProperty("excluded_locations")
    private List<String> excludedLocations;
    @JsonProperty("formats")
    private List<String> formats;
    @JsonProperty("video_id")
    private String videoId;
    @JsonProperty("description")
    private String description;
    @JsonProperty("title")
    private String title;
    @JsonProperty("goal_period")
    private String goalPeriod;
    @JsonProperty("goal_views")
    private Float goalViews;
    @JsonProperty("duration")
    private Integer duration;
    @JsonProperty("clicks")
    private Integer clicks;
    @JsonProperty("views")
    private Integer views;
    @JsonProperty("impressions")
    private Integer impressions;
    @JsonProperty("excluded_categories")
    private List<String> excludedCategories;
    @JsonProperty("devices")
    private List<String> devices;
    @JsonProperty("categories")
    private List<String> categories;
    @JsonProperty("cpc")
    private Long cpc;
    @JsonProperty("cpv")
    private Long cpv;
    @JsonProperty("internal_cpv")
    private Long internaCpv;
    @JsonProperty("currency")
    private String currency;
    @JsonProperty("schedules")
    private Integer[] schedules;
    @JsonProperty("timetable")
    private List<String> timetable;
    @JsonProperty("start_date")
    private String startDate;
    @JsonProperty("end_date")
    private String endDate;
    @JsonProperty("_updated")
    private String updated;
    @JsonProperty("_created")
    private String created;
    @JsonProperty("delivery")
    private String delivery;
    @JsonProperty("resizable_thumbnail_url")
    private String resizableThumbnailUrl;
    @JsonProperty("custom_video_url")
    private String customVideoUrl;
    @JsonProperty("paused")
    private Boolean paused;
    @JsonProperty("domain_blacklist")
    private List<String> domainBlacklist;
    @JsonProperty("domain_whitelist")
    private List<String> domainWhitelist;
}
