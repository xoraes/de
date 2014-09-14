package delib

import (
	"time"
)

type AdUnits struct {
	Items []Unit `json:"_items"`
}
type Unit struct {
	Id                 string     `json:"_id,omitempty"`
	Updated            *time.Time `json:"_updated,omitempty"`
	Created            *time.Time `json:"_created,omitempty"`
	Ad                 string     `json:"ad,omitempty"`
	CampaignId         string     `json:"campaign,omitempty"`
	TacticId           string     `json:"tactic,omitempty"`
	Channel            string     `json:"channel,omitempty"`
	Account            string     `json:"account,omitempty"`
	Languages          []string   `json:"languages,omitempty"`
	Locations          []string   `json:"locations,omitempty"`
	ExcludedLocations  []string   `json:"excluded_locations,omitempty"`
	AdFormats          []string   `json:"formats,omitempty"`
	VideoUrl           string     `json:"video_url,omitempty"`
	ThumbnailUrl       string     `json:"thumbnail_url,omitempty"`
	Description        string     `json:"description,omitempty"`
	Title              string     `json:"title,omitempty"`
	ChannelUrl         string     `json:"channel_url,omitempty"`
	Status             string     `json:"status,omitempty"`
	GoalPeriod         string     `json:"goal_period,omitempty"`
	GoalViews          float64    `json:"goal_views,omitempty"`
	Duration           uint32     `json:"duration,omitempty"`
	Clicks             float64    `json:"clicks,omitempty"`
	Views              float64    `json:"views,omitempty"`
	ExcludedCategories []string   `json:"excluded_categories,omitempty"`
	Devices            []string   `json:"devices,omitempty"`
	Categories         []string   `json:"categories,omitempty"`
	Cpc                float32    `json:"cpc,omitempty"`
	GoalReached        bool       `json:"goal_reached,omitempty"`
}

type SearchQuery struct {
	Languages               []string `json:"languages,omitempty"`
	Locations               []string `json:"locations,omitempty"`
	Categories              []string `json:"categories,omitempty"`
	Device                  string   `json:"device,omitempty"`
	AdFormat                string   `json:"format,omitempty"`
	DisableIncludes         bool
	DisableActiveCheck      bool
	DisableGoalReachedCheck bool
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
