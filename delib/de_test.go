package delib

import (
	"encoding/json"
	"fmt"

	"log"
	"testing"
	"time"
)

func NewData(id string, campaignId string) map[string]interface{} {
	var dst = make(map[string]interface{})
	var (
		timenow = time.Now().Format(time.RFC3339)
		data    = map[string]interface{}{
			"_id":                 id,
			"ad":                  id,
			"duration":            123,
			"campaign":            campaignId,
			"_updated":            timenow,
			"_created":            timenow,
			"categories":          []string{"cat1", "cat2"},
			"devices":             []string{"dev1", "dev2"},
			"title":               "title",
			"description":         "description",
			"channel":             "channel",
			"channel_url":         "http://channel-url",
			"formats":             []string{"format1", "format2"},
			"thumbnail_url":       "http://thumbnail-url",
			"video_url":           "http://video-url",
			"tactic":              "1",
			"status":              "active",
			"account":             "1",
			"goal_views":          1999999,
			"goal_period":         "total",
			"cpc":                 1.12,
			"locations":           []string{"us", "fr"},
			"languages":           []string{"en", "fr"},
			"excluded_categories": []string{"ec1", "ec2"},
			"excluded_locations":  []string{"el1", "el2"},
		}
	)
	for k, v := range data {
		dst[k] = v
	}
	return dst
}

func TestReturnOnlyActive(t *testing.T) {
	d1 := NewData("1", "1")
	d1["status"] = "inactive"
	d2 := NewData("2", "2")
	defer clean("1", t)
	defer clean("2", t)
	loadData(d1, t)
	in2 := loadData(d2, t)

	var sq SearchQuery
	//building search via map will test actual json input is as expected
	search := map[string]interface{}{
		"device":     "dev1",
		"locations":  []string{"fr"},
		"languages":  []string{"en"},
		"format":     "format1",
		"categories": []string{"cat1"},
	}
	sqb, e1 := json.Marshal(search)
	if e1 != nil {
		t.Fail()
	}
	e2 := json.Unmarshal(sqb, &sq)
	if e2 != nil {
		t.Fail()
	}
	//wait for data to be loaded
	time.Sleep(2 * time.Second)
	adunitOut, err := QueryUniqAdFromES(20, sq)
	if err != nil {
		t.Fail()
	}
	Assert(len(adunitOut) == 1, t, "testing", nil)
	deepEqual(in2, &adunitOut[0], t)
}

func TestInsertAndQuery(t *testing.T) {
	d := NewData("1", "1")
	defer clean("1", t)
	in := loadData(d, t)
	var sq SearchQuery
	//building search via map will test actual json input is as expected
	search := map[string]interface{}{
		"device":     "dev1",
		"locations":  []string{"fr"},
		"languages":  []string{"en"},
		"format":     "format1",
		"categories": []string{"cat1"},
	}
	sqb, e1 := json.Marshal(search)
	if e1 != nil {
		t.Fail()
	}
	e2 := json.Unmarshal(sqb, &sq)
	if e2 != nil {
		t.Fail()
	}
	//wait for data to be loaded
	time.Sleep(2 * time.Second)
	adunitOut, err := QueryUniqAdFromES(20, sq)
	if err != nil {
		t.Fail()
	}
	Assert(len(adunitOut) == 1, t, "testing", nil)
	deepEqual(in, &adunitOut[0], t)
}
func TestAllMatch(t *testing.T) {
	d := NewData("1", "1")
	d["categories"] = nil
	d["locations"] = nil
	d["languages"] = nil
	d["formats"] = nil
	d["devices"] = nil

	defer clean("1", t)
	loadData(d, t)
	sq := SearchQuery{}

	//wait for data to be loaded
	time.Sleep(2 * time.Second)
	adunitOut, err := QueryUniqAdFromES(1, sq)
	if err != nil {
		t.Fail()
	}
	Assert(len(adunitOut) == 1, t, "testing", nil)
}

func TestDuplicateCampaigns(t *testing.T) {
	d1 := NewData("1", "1")
	d2 := NewData("2", "1")
	defer clean("1", t)
	defer clean("2", t)
	in1 := loadData(d1, t)
	loadData(d2, t)
	sq := SearchQuery{
		Device:     "dev1",
		Locations:  []string{"fr"},
		Languages:  []string{"en"},
		AdFormat:   "format1",
		Categories: []string{"cat1"},
	}
	//wait for data to be loaded
	time.Sleep(2 * time.Second)
	unitsOut, err := QueryUniqAdFromES(2, sq)
	if err != nil {
		t.Fail()
	}
	Assert(len(unitsOut) == 1, t, "testing", nil)
	deepEqual(in1, &unitsOut[0], t)
}

func deepEqual(a *Unit, b *Unit, t *testing.T) {
	//Assert(a.GoalViews == b.GoalViews, t, "testing", nil)
	//Assert(a.Id == b.Id, t, "testing", nil)
	//Assert(a.Ad == b.Ad, t, "testing", nil)
	Assert(a.TacticId == b.TacticId, t, "testing", nil)
	Assert(a.CampaignId == b.CampaignId, t, "testing", nil)
	Assert(a.Channel == b.Channel, t, "testing", nil)
	Assert(a.Description == b.Description, t, "testing", nil)
	Assert(a.Account == b.Account, t, "testing", nil)
	Assert(a.Cpc == b.Cpc, t, "testing", nil)
	Assert(a.ChannelUrl == b.ChannelUrl, t, "testing", nil)
	Assert(a.ThumbnailUrl == a.ThumbnailUrl, t, "testing", nil)
	Assert(a.VideoUrl == b.VideoUrl, t, "testing", nil)
	Assert(a.Account == b.Account, t, "testing", nil)
	//Assert(a.GoalPeriod == b.GoalPeriod, t, "testing", nil)
	//Assert(a.Status == b.Status, t, "testing", nil)
	Assert(a.Title == b.Title, t, "testing", nil)
	//Assert(a.Created.Equal(*b.Created), t, "testing", nil)
	//Assert(a.Updated.Equal(*b.Updated), t, "testing", nil)
	Assert(a.Duration == b.Duration, t, "testing", nil)
	//Assert(len(a.Locations) == len(b.Locations), t, "testing", nil)
	//Assert(len(a.Languages) == len(b.Languages), t, "testing", nil)
	//Assert(len(a.AdFormats) == len(b.AdFormats), t, "testing", nil)
	//Assert(len(a.Devices) == len(b.Devices), t, "testing", nil)
	//Assert(len(a.Categories) == len(b.Categories), t, "testing", nil)
	//Assert(len(a.ExcludedCategories) == len(b.ExcludedCategories), t, "testing", nil)
	//Assert(len(a.ExcludedLocations) == len(b.ExcludedLocations), t, "testing", nil)

}

// dumb simple assert for testing, printing
//    Assert(len(items) == 9, t, "Should be 9 but was %d", len(items))
func Assert(is bool, t *testing.T, format string, args ...interface{}) {
	if is == false {
		log.Printf(format, args...)
		t.Fail()
	}
}

func printAdUnit(ad *Unit) {
	b, err := json.MarshalIndent(ad, "", "    ")
	if err == nil {
		fmt.Println(string(b))
	}
}

func loadData(v interface{}, t *testing.T) *Unit {
	var (
		in Unit
	)
	br1, _ := json.Marshal(v)
	err1 := json.Unmarshal(br1, &in)
	if err1 != nil {
		t.Fail()
	}
	err2 := InsertAdUnit(&in)
	if err2 != nil {
		t.Fail()
	}
	return &in

}
func init() {
	fmt.Println("Creating Index")
	CreateIndex()
	time.Sleep(3 * time.Second)
}

func clean(id string, t *testing.T) {
	if found, err := DeleteAdUnitById(id); err != nil || !found {
		t.Fail()
	}
}
