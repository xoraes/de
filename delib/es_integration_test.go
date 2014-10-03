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
			"goal_views":          9,
			"goal_period":         "total",
			"cpc":                 12,
			"schedules":           []uint{16777215, 16777215, 16777215, 16777215, 16777215, 16777215, 16777215},
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
	loadData(t, d1, d2)

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
	wait()
	adunitOut, err := QueryUniqAdFromES(20, sq)
	if err != nil {
		t.Fail()
	}
	Assert(len(adunitOut) == 1, t, "testing", nil)
}

func TestInsertAndQuery(t *testing.T) {
	d := NewData("1", "1")
	loadData(t, d)
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
	wait()
	adunitOut, err := QueryUniqAdFromES(20, sq)
	if err != nil {
		t.Fail()
	}
	Assert(len(adunitOut) == 1, t, "testing", nil)
}
func TestAllMatch(t *testing.T) {
	d := NewData("1", "1")
	d["categories"] = nil
	d["locations"] = nil
	d["languages"] = nil
	d["formats"] = nil
	d["devices"] = nil
	loadData(t, d)
	sq := SearchQuery{}

	//wait for data to be loaded
	wait()
	adunitOut, err := QueryUniqAdFromES(1, sq)
	if err != nil {
		t.Fail()
	}
	Assert(len(adunitOut) == 1, t, "testing", nil)
}
func TestGoalReached(t *testing.T) {
	d1 := NewData("1", "1")
	d2 := NewData("2", "2")
	d1["goal_reached"] = true
	loadData(t, d1, d2)
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
	wait()
	adunitOut, err := QueryUniqAdFromES(20, sq)
	if err != nil {
		t.Fail()
	}
	Assert(len(adunitOut) == 1, t, "return only one ad unit", len(adunitOut))
}

func TestGetLastUpdated(t *testing.T) {
	clean()
	ti := GetESLastUpdated()
	var time2 = time.Time{}
	Assert(ti.Equal(time2), t, "time should be zero here", ti)
	d := NewData("1", "1")
	loadData(t, d)
	wait()
	ti = GetESLastUpdated()
	Assert(!ti.IsZero() && ti.Before(time.Now()) == true, t, "time should be zero here", ti)
}
func TestGetAllAdUnits(t *testing.T) {
	d1 := NewData("1", "1")
	d2 := NewData("2", "1")
	d3 := NewData("3", "1")
	d4 := NewData("4", "1")
	d1["status"] = "inactive"
	d1["goal_reached"] = true
	d2["status"] = "inactive"
	d2["goal_reached"] = true
	d3["status"] = "inactive"
	d3["goal_reached"] = true
	d4["status"] = "inactive"
	d4["goal_reached"] = true
	loadData(t, d1, d2, d3, d4)
	//wait for data to be fully loaded
	wait()
	units, _ := GetAllAdUnits()
	Assert(len(units) == 4, t, "testing", len(units))
}

func TestGetAdUnitsByCampaign_InvalidId(t *testing.T) {
	units, err := GetAdUnitsByCampaign("INVALID")
	if err != nil {
		t.Error(err)
	}
	Assert(len(units) == 0, t, "ad units should be", len(units))
}
func TestGetAdUnitsByCampaign(t *testing.T) {
	d1 := NewData("1", "1")
	d2 := NewData("2", "1")
	loadData(t, d1, d2)
	wait()
	units, err := GetAdUnitsByCampaign("1")
	if err != nil {
		t.Error(err)
	}
	Assert(len(units) == 2, t, "expected 2, returned ad units return were ", len(units))
}
func TestStatusInactive(t *testing.T) {
	var sq SearchQuery
	d1 := NewData("1", "1")
	d2 := NewData("2", "2")
	d1["status"] = "inactive"
	loadData(t, d1, d2)
	search := map[string]interface{}{
		"device":     "dev1",
		"locations":  []string{"fr"},
		"languages":  []string{"en"},
		"format":     "format1",
		"categories": []string{"cat1"},
	}
	sqb, e1 := json.Marshal(search)
	e2 := json.Unmarshal(sqb, &sq)
	if e2 != nil {
		t.Fail()
	}
	if e1 != nil {
		t.Fail()
	}
	//wait for data to be fully loaded
	wait()
	units, _ := QueryUniqAdFromES(2, sq)
	Assert(len(units) == 1, t, "testing", len(units))
}
func TestDuplicateCampaigns(t *testing.T) {
	d1 := NewData("1", "1")
	d2 := NewData("2", "1")
	loadData(t, d1, d2)

	sq := SearchQuery{
		Device:     "dev1",
		Locations:  []string{"fr"},
		Languages:  []string{"en"},
		AdFormat:   "format1",
		Categories: []string{"cat1"},
	}
	//wait for data to be loaded
	wait()
	unitsOut, err := QueryUniqAdFromES(2, sq)
	if err != nil {
		t.Fail()
	}
	Assert(len(unitsOut) == 1, t, "testing", nil)
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
	Assert(a.GoalReached == b.GoalReached, t, "testing", nil)

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

func loadData(t *testing.T, v ...interface{}) *Unit {
	clean()
	for _, val := range v {
		var (
			in  Unit
			in2 Unit
		)
		br1, _ := json.Marshal(val)
		err1 := json.Unmarshal(br1, &in)
		if err1 != nil {
			log.Println(err1)
			t.Fail()
		}
		err2 := InsertAdUnit(&in)
		if err2 != nil {
			log.Println(err2)
			t.Fail()
		}
		br2, err3 := GetAdUnitById(in.Id)
		if err3 != nil {
			log.Println(err3)
			t.Fail()
		}
		err4 := json.Unmarshal(br2, &in2)
		if err4 != nil {
			log.Println(err4)
			t.Fail()
		}
		deepEqual(&in, &in2, t)
	}

	return nil

}
func init() {
	fmt.Println("Creating Index")
	clean()
	wait()
}

func clean() {
	DeleteIndex()
	CreateIndex()
}
func wait() {
	time.Sleep(2 * time.Second)
}
