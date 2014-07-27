package main

import (
	"flag"
	de "github.com/dailymotion/pixelle-de/delib"
	"github.com/mattbaird/elastigo/api"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"time"
)

//convert campaign collection data to Ad so it can be index into ES
//with relevant info preserved
func indexAdFromCampaignData(c *de.Ad, ad_id string) *de.DeError {
	var ad de.Ad

	ad.Id = bson.ObjectIdHex(ad_id)
	//c.paused represents campaign paused.
	ad.CampaignPaused = c.Paused
	//c.Updated represents campaign updated
	ad.CampaignUpdated = c.Updated
	ad.Locations = c.Locations
	ad.Account = c.Account
	ad.ExcludedLocations = c.ExcludedLocations
	ad.GoalViews = c.GoalViews
	ad.GoalPeriod = c.GoalPeriod

	return de.UpdateAd(&ad)
}

// DE servers run a process every X seconds (default 60 secs) to query data from campaign db and
// update themselves with new/update campaign information from campaign db.

func main() {
	var (
		url    string
		repeat int
	)

	flag.StringVar(&url, "url", "mongodb://localhost/pixelle", "mongodb dial url")
	flag.IntVar(&repeat, "repeat", 60, "time interval in seconds for sync to query mongodb")

	flag.Parse()

	api.Domain = "0.0.0.0"
	api.Port = "9200"

	// Create a session which maintains a pool of socket connections
	// to our MongoDB.
	mongoSession, err := mgo.Dial(url)
	if err != nil {
		log.Fatalf("CreateSession: %s\n", err)
	}

	// Reads may not be entirely up-to-date, but they will always see the
	// history of changes moving forward, the data read will be consistent
	// across sequential queries in the same session, and modifications made
	// within the session will be observed in following queries (read-your-writes).
	// http://godoc.org/labix.org/v2/mgo#Session.SetMode
	mongoSession.SetMode(mgo.Monotonic, true)
	var updatedAds, updatedCampaigns []de.Ad

	var last time.Time
	for {
		//Update Ad collection data from mongo to ES
		last = de.GetESLastUpdated("_updated_ad")
		log.Println("Last ad updated timestamp:", last.Format(time.RFC3339))

		if updatedAds, err = de.GetLastUpdated(mongoSession, "ads", last); err != nil {
			log.Println("Error while getting latest ads data from mongodb. Last updated timestamp on ES: ", last)
		}
		log.Println("1. Num ads to update", len(updatedAds))
		for _, value := range updatedAds {
			de.UpdateAd(&value)
		}

		//sleep for a bit for ES to catch up
		time.Sleep(5 * time.Second)

		//Update campaign data from mongo to ES
		last = de.GetESLastUpdated("_updated_campaign")
		log.Println("Last campaign updated timestamp:", last.Format(time.RFC3339))
		if updatedCampaigns, err = de.GetLastUpdated(mongoSession, "campaigns", last); err != nil {
			log.Println("Error while getting latest campaigns data from mongodb. Last updated timestamp on ES: ", last)
		}
		log.Println("2. Num campaigns to update", len(updatedCampaigns))
		for _, v := range updatedCampaigns {
			// here the v.AdId is really the campaign Id
			if adIds, err := de.GetAdIdsByCampaign(v.Id.Hex()); err != nil {
				panic(err.Error())
			} else {
				for _, val := range adIds {
					if err = indexAdFromCampaignData(&v, val); err != nil {
						panic(err.Error())
					}
				}
			}
		}

		log.Println("Sync Completed")

		time.Sleep(time.Duration(60) * time.Second)
	}
}
