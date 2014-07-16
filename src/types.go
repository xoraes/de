package main

import (
	"labix.org/v2/mgo/bson"
	"time"
)

// add single go struct entity
type Ad struct {
	AdId              string   `json:"ad_id,omitempty"`
	CampaignId        string   `json:"campaign_id,omitempty"`
	Languages         []string `json:"languages,omitempty"`
	Locations         []string `json:"locations,omitempty"`
	ExcludedLocations []string `json:"excluded_locations,omitempty"`
	AdFormats         []int    `json:"ad_formats,omitempty"`
	Account           string   `json:"account_id,omitempty"`
	VideoUrl          string   `json:"video_url,omitempty"`
	ThumbnailUrl      string   `json:"thumbnail_url,omitempty"`
	Description       string   `json:"description,omitempty"`
	Title             string   `json:"title,omitempty"`
	TacticId          string   `json:"tactic_id,omitempty"`
	ChannelId         string   `json:"channel_id,omitempty"`
	ChannelUrl        string   `json:"channel_url,omitempty"`
	IsPaused          bool     `json:"is_paused,omitempty"`
	GoalReached       bool     `json:"goal_reached,omitempty"`
	Updated       time.Time     `json:"updated_at,omitempty"`
}
type CampaignDb struct {
	Id                bson.ObjectId `bson:"_id,omitempty"`
	Updated           time.Time     `bson:"_updated,omitempty"`
	Created           time.Time     `bson:"_created,omitempty"`
	Paused            bool          `bson:"Paused,omitempty"`
	GoalPeriod        string        `bson:"goal_period,omitempty"`
	GoalViews         int           `bson:"goal_views,omitempty"`
	Name              string        `bson:"name,omitempty"`
	Locations         []string      `bson:"locations,omitempty"`
	ExcludedLocations []string      `bson:"excluded_locations,omitempty"`
	Account           string        `bson:"account_id,omitempty"`
}
type SearchQuery struct {
	Languages []string `json:"languages,omitempty"`
	Locations []string `json:"locations,omitempty"`
	AdFormat  int      `json:"ad_format,omitempty"`
}
type SearchResponse struct {
	UpdatedTS time.Time `json:"updated_at,omitempty"`
}
