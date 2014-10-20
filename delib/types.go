package delib

import (
	"encoding/json"
	"time"
)

type AdUnits struct {
	Items []Unit `json:"_items"`
}
type Unit struct {
	Id                      string     `json:"_id,omitempty"`
	Updated                 *time.Time `json:"_updated,omitempty"`
	Created                 *time.Time `json:"_created,omitempty"`
	Ad                      string     `json:"ad,omitempty"`
	CampaignId              string     `json:"campaign,omitempty"`
	TacticId                string     `json:"tactic,omitempty"`
	Channel                 string     `json:"channel,omitempty"`
	Account                 string     `json:"account,omitempty"`
	Languages               []string   `json:"languages,omitempty"`
	Locations               []string   `json:"locations,omitempty"`
	ExcludedLocations       []string   `json:"excluded_locations,omitempty"`
	AdFormats               []string   `json:"formats,omitempty"`
	VideoUrl                string     `json:"video_url,omitempty"`
	ThumbnailUrl            string     `json:"thumbnail_url,omitempty"`
	Description             string     `json:"description,omitempty"`
	Title                   string     `json:"title,omitempty"`
	ChannelUrl              string     `json:"channel_url,omitempty"`
	Status                  string     `json:"status,omitempty"`
	GoalPeriod              string     `json:"goal_period,omitempty"`
	GoalViews               float64    `json:"goal_views,omitempty"`
	Duration                uint32     `json:"duration,omitempty"`
	Clicks                  float64    `json:"clicks,omitempty"`
	Views                   float64    `json:"views,omitempty"`
	ExcludedCategories      []string   `json:"excluded_categories,omitempty"`
	Devices                 []string   `json:"devices,omitempty"`
	Categories              []string   `json:"categories,omitempty"`
	Cpc                     uint64     `json:"cpc,omitempty"`
	Schedules               []uint     `json:"schedules,omitempty"`
	Timetable               []string   `json:"timetable,omitempty"`
	StartDate               *jTime     `json:"start_date,omitempty"`
	EndDate                 *jTime     `json:"end_date,omitempty"`
	Delivery                string     `json:"delivery,omitempty"`
	Resizable_thumbnail_url string     `json:"resizable_thumbnail_url,omitempty"`
	// bool types should be never omitempty otherwise false value
	// is never written back since zero value for bool is false :(
	Paused      bool `json:"paused"`
	GoalReached bool `json:"goal_reached"`
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
	DisablePausedCheck      bool
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
func (jt *jTime) Unix() int64 {
	return jt.t.Unix()
}
func (jt *jTime) UTC() time.Time {
	return jt.t.UTC()
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
