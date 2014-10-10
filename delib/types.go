package delib

import (
	"encoding/json"
	"time"
)

type AdUnits struct {
	Items []Unit `json:"_items"`
}
type Unit struct {
	Id                 string     `json:"_id"`
	Updated            *time.Time `json:"_updated"`
	Created            *time.Time `json:"_created"`
	Ad                 string     `json:"ad"`
	CampaignId         string     `json:"campaign"`
	TacticId           string     `json:"tactic"`
	Channel            string     `json:"channel"`
	Account            string     `json:"account"`
	Languages          []string   `json:"languages"`
	Locations          []string   `json:"locations"`
	ExcludedLocations  []string   `json:"excluded_locations"`
	AdFormats          []string   `json:"formats"`
	VideoUrl           string     `json:"video_url"`
	ThumbnailUrl       string     `json:"thumbnail_url"`
	Description        string     `json:"description"`
	Title              string     `json:"title"`
	ChannelUrl         string     `json:"channel_url"`
	Status             string     `json:"status"`
	GoalPeriod         string     `json:"goal_period"`
	GoalViews          float64    `json:"goal_views"`
	Duration           uint32     `json:"duration"`
	Clicks             float64    `json:"clicks"`
	Views              float64    `json:"views"`
	ExcludedCategories []string   `json:"excluded_categories"`
	Devices            []string   `json:"devices"`
	Categories         []string   `json:"categories"`
	Cpc                uint64     `json:"cpc"`
	GoalReached        bool       `json:"goal_reached"`
	Schedules          []uint     `json:"schedules,omitempty"`
	Timetable          []string   `json:"timetable"`
	StartDate          *jTime     `json:"start_date"`
	EndDate            *jTime     `json:"end_date"`
}

type SearchQuery struct {
	Languages               []string `json:"languages,omitempty"`
	Locations               []string `json:"locations,omitempty"`
	Categories              []string `json:"categories,omitempty"`
	Device                  string   `json:"device,omitempty"`
	AdFormat                string   `json:"format,omitempty"`
	Time                    *jTime   `json:"time,omitempty"`
	TimeTable               []string `json:"timetable,omitempty"`
	DisableIncludes         bool
	DisableActiveCheck      bool
	DisableGoalReachedCheck bool
	DisableDateCheck        bool
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

type jTime struct {
	t time.Time
}

// MarshalJSON implements the json.Marshaler interface.
// The time is a quoted string in RFC 3339 format, with sub-second precision added if present.
func (jt *jTime) MarshalJSON() ([]byte, error) {
	if _, err := jt.t.MarshalJSON(); err != nil {
		return nil, err
	}
	return []byte(jt.t.Format(`"` + time.RFC3339 + `"`)), nil
}
func (jt *jTime) String() string {

	if jt == nil {
		return ""
	} else {
		return jt.t.Format(time.RFC3339)
	}
}
func (jt *jTime) Weekday() string {
	return jt.t.Weekday().String()
}
func (jt *jTime) Hour() int {
	return jt.t.Hour()
}
func (jt *jTime) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return err
	}
	*jt = jTime{t: t}
	return nil
}
