package main

import (
	"flag"
	de "github.com/dailymotion/pixelle-de/delib"
	"log"
	"time"
)

// DE servers run a process every X seconds (default 60 secs) to query data from campaign db and
// update themselves with new/update campaign information from campaign db.

func main() {
	var (
		repeat  int
		err     error
		adUnits *de.AdUnits
		last    time.Time
	)
	flag.IntVar(&repeat, "repeat", 60, "time interval in seconds for sync to query mongodb")

	flag.Parse()
	for {
		//Update Ad collection data from api to ES
		last = de.GetESLastUpdated("_updated")
		log.Println("Last ad unit updated timestamp:", last)

		if adUnits, err = de.GetUpdatedAdUnits(last); err != nil {
			log.Println("Error obtaining/marshalling ad Units from API", err)
		}
		if adUnits != nil && adUnits.Items != nil && len(adUnits.Items) > 0 {
			for _, u := range adUnits.Items {
				if u.Status == "active" {
					de.UpdateAd(&u)
				}
				if u.Status == "deleted" {
					de.DeleteAdUnitById(u.Id)
					log.Println("deleted ad unit: " + u.Id)
				}

			}
			log.Println("Sync Completed")
		} else {
			log.Println("Nothing to Sync")
		}
		time.Sleep(time.Duration(repeat) * time.Second)
	}
}
