package com.dailymotion.pixelle.deserver.model;

import com.dailymotion.pixelle.deserver.logger.InjectLogger;
import com.dailymotion.pixelle.deserver.processor.DeException;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;

import java.util.List;

/**
 * Created by n.dhupia on 11/5/14.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class AdUnit {
    @InjectLogger
    private static Logger logger;
    @JsonProperty("_id")
    private String id;
    @JsonProperty("ad")
    private String ad;
    @JsonProperty("campaign")
    private String campaignId;
    @JsonProperty("tactic")
    private String tacticId;
    @JsonProperty("channel")
    private String channelId;
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
    @JsonProperty("video_url")
    private String videoUrl;
    @JsonProperty("video_id")
    private String videoId;
    @JsonProperty("thumbnail_url")
    private String thumbnailUrl;
    @JsonProperty("description")
    private String description;
    @JsonProperty("title")
    private String title;
    @JsonProperty("channel_url")
    private String channelUrl;
    @JsonProperty("status")
    private String status;
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
    @JsonProperty("excluded_categories")
    private List<String> excludedCategories;
    @JsonProperty("devices")
    private List<String> devices;
    @JsonProperty("categories")
    private List<String> categories;
    @JsonProperty("cpc")
    private Long cpc;
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
    @JsonProperty("resizeable_thumbnail_url")
    private String resizableThumbnailUrl;
    @JsonProperty("goal_reached")
    private Boolean goalReached;
    @JsonProperty("paused")
    private Boolean paused;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

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

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public List<String> getLanguages() {
        return languages;
    }

    public void setLanguages(List<String> languages) {
        this.languages = languages;
    }

    public List<String> getLocations() {
        return locations;
    }

    public void setLocations(List<String> locations) {
        this.locations = locations;
    }

    public List<String> getExcludedLocations() {
        return excludedLocations;
    }

    public void setExcludedLocations(List<String> excludedLocations) {
        this.excludedLocations = excludedLocations;
    }

    public List<String> getFormats() {
        return formats;
    }

    public void setFormats(List<String> formats) {
        this.formats = formats;
    }

    public String getVideoUrl() {
        return videoUrl;
    }

    public void setVideoUrl(String videoUrl) {
        this.videoUrl = videoUrl;
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

    public String getChannelUrl() {
        return channelUrl;
    }

    public void setChannelUrl(String channelUrl) {
        this.channelUrl = channelUrl;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getGoalPeriod() {
        return goalPeriod;
    }

    public void setGoalPeriod(String goalPeriod) {
        this.goalPeriod = goalPeriod;
    }

    public Float getGoalViews() {
        return goalViews;
    }

    public void setGoalViews(Float goalViews) {
        this.goalViews = goalViews;
    }

    public Integer getDuration() {
        return duration;
    }

    public void setDuration(Integer duration) {
        this.duration = duration;
    }

    public Integer getClicks() {
        return clicks;
    }

    public void setClicks(Integer clicks) {
        this.clicks = clicks;
    }

    public Integer getViews() {
        return views;
    }

    public void setViews(Integer views) {
        this.views = views;
    }

    public List<String> getExcludedCategories() {
        return excludedCategories;
    }

    public void setExcludedCategories(List<String> excludedCategories) {
        this.excludedCategories = excludedCategories;
    }

    public List<String> getDevices() {
        return devices;
    }

    public void setDevices(List<String> devices) {
        this.devices = devices;
    }

    public List<String> getCategories() {
        return categories;
    }

    public void setCategories(List<String> categories) {
        this.categories = categories;
    }

    public Long getCpc() {
        return cpc;
    }

    public void setCpc(Long cpc) {
        this.cpc = cpc;
    }

    public Integer[] getSchedules() {
        return schedules;
    }

    public void setSchedules(Integer[] schedules) {
        this.schedules = schedules;
    }

    public List<String> getTimetable() {
        return timetable;
    }

    public void setTimetable(List<String> timetable) {
        this.timetable = timetable;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public String getUpdated() {
        return updated;
    }

    public void setUpdated(String updated) {
        this.updated = updated;
    }

    public String getCreated() {
        return created;
    }

    public void setCreated(String created) {
        this.created = created;
    }

    public String getDelivery() {
        return delivery;
    }

    public void setDelivery(String delivery) {
        this.delivery = delivery;
    }

    public String getResizableThumbnailUrl() {
        return resizableThumbnailUrl;
    }

    public void setResizableThumbnailUrl(String resizableThumbnailUrl) {
        this.resizableThumbnailUrl = resizableThumbnailUrl;
    }

    public Boolean getGoalReached() {
        return goalReached;
    }

    public void setGoalReached(Boolean goalReached) {
        this.goalReached = goalReached;
    }

    public Boolean getPaused() {
        return paused;
    }

    public void setPaused(Boolean paused) {
        this.paused = paused;
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