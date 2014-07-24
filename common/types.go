package common

import (
	"labix.org/v2/mgo/bson"
	"time"
)

type Ad struct {
	AdId              bson.ObjectId `bson:"_id,omitempty" json:"_id,omitempty"`
	CampaignId        bson.ObjectId `bson:"campaign,omitempty" json:"campaign,omitempty"`
	TacticId          bson.ObjectId `bson:"tactic,omitempty" json:"tactic,omitempty"`
	ChannelId         bson.ObjectId `bson:"channel,omitempty" json:"channel,omitempty"`
	Account           bson.ObjectId `bson:"account,omitempty" json:"account,omitempty"`
	Languages         []string      `bson:"languages,omitempty" json:"languages,omitempty"`
	Locations         []string      `bson:"locations,omitempty" json:"locations,omitempty"`
	ExcludedLocations []string      `bson:"excluded_locations,omitempty" json:"excluded_locations,omitempty"`
	AdFormats         []int         `bson:"ad_formats,omitempty" json:"ad_formats,omitempty"`
	VideoUrl          string        `bson:"video_url,omitempty" json:"video_url,omitempty"`
	ThumbnailUrl      string        `bson:"thumbnail_url,omitempty" json:"thumbnail_url,omitempty"`
	Description       string        `bson:"description,omitempty" json:"description,omitempty"`
	Title             string        `bson:"title,omitempty" json:"title,omitempty"`
	ChannelUrl        string        `bson:"channel_url,omitempty" json:"channel_url,omitempty"`
	Status            string        `bson:"status,omitempty" json:"status,omitempty"`
	GoalPeriod        string        `bson:"goal_period,omitempty" json:"goal_period,omitempty"`
	GoalViews         int           `bson:"goal_views,omitempty" json:"goal_views,omitempty"`
	Duration          int           `bson:"duration,omitempty" json:"duration,omitempty"`
	Updated           *time.Time    `bson:"_updated,omitempty" json:"_updated_ad,omitempty"`
	Created           *time.Time    `bson:"_created,omitempty" json:"_created,omitempty"`
	CampaignUpdated   *time.Time    `json:"_updated_campaign,omitempty"`
	CampaignPaused    bool          `json:"paused_campaign,omitempty"`
	Paused            bool          `bson:"paused" json:"paused_ad,omitempty"`
}

type SearchQuery struct {
	Languages []string `json:"languages,omitempty"`
	Locations []string `json:"locations,omitempty"`
	AdFormat  int      `json:"ad_format,omitempty"`
}

type SearchQueryResponse struct {
	AdUnits []Ad `json:"_units,omitempty"`
}
type DeError struct {
	Msg  string `json:"message,omitempty"`
	Code int    `json:"status,omitempty"`
}

func (e DeError) Error() string {
	return e.Msg
}
func (e DeError) ErrorCode() int {
	return e.Code
}
func NewError(code int, v interface{}) *DeError {
	if err, ok := v.(error); ok {
		return &DeError{Code: code, Msg: err.Error()}
	} else if str, ok := v.(string); ok {
		return &DeError{Code: code, Msg: str}
	}
	return &DeError{Code: code, Msg: ""}
}
