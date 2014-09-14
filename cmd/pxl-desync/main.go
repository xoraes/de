package main

import (
	"flag"
	"github.com/dailymotion/pixelle-analytics-consumer/pacdal"
	de "github.com/dailymotion/pixelle-de/delib"
	"github.com/golang/glog"
	"log"
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
	var (
		err     error
		adUnits *de.AdUnits
		last    time.Time
	)
	//flag.Parse should always be in main not init()
	flag.Parse()

	if cass, err = pacdal.NewAnalyticsDbSession(keyspace); err != nil {
		log.Fatal("Error creating cassandra session -- ", err)
	}
	//remove the index
	de.DeleteIndex()
	//create the ES index
	de.CreateIndex()

	for {
		//Update Ad collection data from api to ES
		last = de.GetESLastUpdated()
		log.Println("Last ad unit updated timestamp:", last)

		if adUnits, err = de.GetUpdatedAdUnits(last); err != nil {
			log.Println("Error obtaining/marshalling ad Units from API", err)
		}
		if adUnits != nil && adUnits.Items != nil && len(adUnits.Items) > 0 {
			for _, u := range adUnits.Items {
				if u.Status != "deleted" { // status is active or inactive i.e no deleted then insert the adunit
					de.InsertAdUnit(&u)
				} else if found, derr := de.DeleteAdUnitById(u.Id); found && derr == nil {
					log.Println("deleted ad unit: " + u.Id)
				}
			}
			log.Println("Sync Completed")
		} else {
			log.Println("Nothing to Sync")
		}
		//update the click count in es for each adunit. If click count > goal, then set GoalReached to true
		if m, casserr := cass.GetClickCountMap(); casserr != nil {
			log.Println("Error connecting to counter db -- ", casserr)
		} else {
			glog.Info("Records received from cassandra:", len(m))
			for k, cnt := range m {
				units, eserr := de.GetAdUnitsByCampaign(k)
				if eserr != nil {
					log.Println("Error connecting to ES", eserr)
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
		//run through all adunits and reset goal reached values based on its current click/view and goal_views
		//this is done so that we can clean up any anomaly and be assured that GoalReached is true
		//if clicks/views have reached goal click/views
		if units, eserr := de.GetAllAdUnits(); eserr != nil {
			log.Println(eserr)
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
		time.Sleep(time.Duration(repeat) * time.Second)
	}
}
