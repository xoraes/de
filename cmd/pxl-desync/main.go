package main

import (
	"flag"
	de "github.com/dailymotion/pixelle-de/delib"
	pacdal "github.com/dailymotion/pixelle-insight/dal"
	"github.com/golang/glog"
	"strings"
	"time"
)

type StringArray []string

func (a *StringArray) Set(s string) error {
	*a = append(*a, s)
	return nil
}
func (a *StringArray) String() string {
	return strings.Join(*a, ",")
}

var (
	keyspace string
	repeat   int
	cass     *pacdal.Cassdao
)

func init() {
	flag.IntVar(&repeat, "repeat", 30, "time interval in seconds for sync to query api")
	flag.StringVar(&keyspace, "cassks", "pxlcounters", "Cassandra keyspace")
}

// DE servers run a process every X seconds (default 60 secs) to query data from campaign db and
// update themselves with new/update campaign information from campaign db.

func main() {
	//flag.Parse should always be in main not init()
	flag.Parse()
	//remove the index
	de.DeleteIndex()
	//create the ES index
	de.CreateIndex()
	var err error
	if cass, err = pacdal.NewAnalyticsDbSession(keyspace); err != nil {
		glog.Error("Error creating cassandra session -- ", err)
	}
	for {
		updateAdUnitsFromApi()
		updateClickCount()
		updateAdUnitsForEvenDistibution()
		updateAllAdUnitsGoalReached()
		time.Sleep(time.Duration(repeat) * time.Second)
	}
}

// Update the click count in es for each adunit based on campaign click count updates from cassandra
// If click count > goal, then set GoalReached to true
func updateClickCount() {
	if m, casserr := cass.GetClickCountMap(); casserr != nil {
		glog.Error("Error connecting to counter db -- ", casserr)
	} else {
		glog.Info("Records received from cassandra:", len(m))
		for k, cnt := range m {
			units, eserr := de.GetAdUnitsByCampaign(k)
			if eserr != nil {
				glog.Error("Error connecting to ES", eserr)
			} else {
				for _, unit := range units {
					if unit.Status == "active" {
						if unit.GoalViews > 0 && cnt >= unit.GoalViews {
							de.UpdateAdUnit(&de.Unit{Id: unit.Id, GoalReached: true, Clicks: cnt})
						} else {
							de.UpdateAdUnit(&de.Unit{Id: unit.Id, Clicks: cnt})
						}
					}
				}
			}
		}
	}
}

func updateAllAdUnitsGoalReached() {
	//run through all adunits and reset goal reached values based on its current click/view and goal_views
	//this is done so that we can clean up any anomaly and be assured that GoalReached is true
	//if clicks/views have reached goal click/views
	//This loop is different from updateClickCount, because in this func we look at all the adunits in ES
	//and update its goal_reached if anything was changed.
	if units, eserr := de.GetAllAdUnits(); eserr != nil {
		glog.Error(eserr)
	} else {
		for _, v := range units {
			if v.GoalViews > 0 && v.GoalViews <= v.Clicks {
				de.UpdateAdUnit(&de.Unit{Id: v.Id, GoalReached: true})
				//incase goalreached was set to true and the goal was increased by api
			} else {
				de.UpdateAdUnit(&de.Unit{Id: v.Id, GoalReached: false})
			}
		}
	}
}

func updateAdUnitsFromApi() {
	var (
		err     error
		adUnits *de.AdUnits
		last    time.Time
	)
	//Update Ad collection data from api to ES
	last = de.GetESLastUpdated()
	glog.Info("Last ad unit updated timestamp:", last)

	if adUnits, err = de.GetUpdatedAdUnits(last); err != nil {
		glog.Error("Error obtaining/marshalling ad Units from API", err)
	}
	if adUnits != nil && adUnits.Items != nil && len(adUnits.Items) > 0 {
		for _, u := range adUnits.Items {
			if u.Status != "deleted" { // status is active or inactive i.e no deleted then insert the adunit
				de.InsertAdUnit(&u)
			} else if found, derr := de.DeleteAdUnitById(u.Id); found && derr == nil {
				glog.Info("deleted ad unit: " + u.Id)
			}
		}
		glog.Info("Sync Completed")
	} else {
		glog.Info("Nothing to Sync")
	}
}

func updateAdUnitsForEvenDistibution() {
	/*while wait(y time) {
	required_run_rate = views_remaining/time_remaining_hours
	current_run_rate = views_in_past_24_hours/24 (day sliding window)

	if current_run_rate > required_run_rate
		pause_ad()
	else
	unpause_ad()
	*/
	const SLIDING_WINDOW_HOURS = 24
	var tr, vr, cr float64
	if units, eserr := de.GetAllAdUnits(); eserr != nil {
		glog.Error(eserr)
	} else {
		for _, v := range units {
			if v.GoalReached != false ||
				v.Delivery != "even" ||
				v.Status != "active" ||
				//end date is really far out (5 years)
				v.EndDate.Unix() > time.Now().AddDate(5, 0, 0).Unix() {
				break
			}
			vr = float64(v.GoalViews - v.Clicks) //TODO change this to view once we do views
			tr = float64(v.EndDate.UTC().Sub(time.Now().UTC()).Hours())
			rr := float64(vr / tr)
			cc, err := cass.GetPastClickCountByHours(v.CampaignId, SLIDING_WINDOW_HOURS*time.Hour)
			if err != nil {
				glog.Error("no-op because of error getting data from cassandra -- " + err.Error())
				break
			}
			//this will return the floor
			cr = float64(cc / SLIDING_WINDOW_HOURS)
			//rr of < 1.0 means number of clicks remaining is less than number of hours remaining,
			//so in this case do not pause the ad and let goal_reached naturally get set to true

			if rr >= 1.0 && cr > rr {
				glog.Info("Pausing adunit => current views: ", v.Clicks, "required views: ", v.GoalViews)
				de.UpdateAdUnit(&de.Unit{Id: v.Id, Paused: true})
			} else {
				glog.Info("Un-Pausing adunit => current views: ", v.Clicks, "required views: ", v.GoalViews)
				de.UpdateAdUnit(&de.Unit{Id: v.Id, Paused: false})
			}
		}

	}
}
