package main

import (
	"encoding/json"
	"flag"
	de "github.com/dailymotion/pixelle-de"
	"github.com/mattbaird/elastigo/api"
	"github.com/mattbaird/elastigo/core"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"time"
)

const (
	PxDb = "pixelle"
)

// RunQuery is a function that is launched as a goroutine to perform
// the MongoDB work.
func RunQuery(mongoSession *mgo.Session, coll string, last time.Time) (iter *mgo.Iter) {

	// Decrement the wait group count so the program knows this
	// has been completed once the goroutine exits.

	// Request a socket connection from the session to process our query.
	// Close the session when the goroutine exits and put the connection back
	// into the pool.
	sessionCopy := mongoSession.Copy()
	defer sessionCopy.Close()

	// Get a collection to execute the query against.
	collection := sessionCopy.DB(PxDb).C(coll)

	iter = collection.Find(bson.M{"_updated": bson.M{"$gt": last}}).Iter()
	return iter
}

func getESLastUpdated(col string) time.Time {
	var err error
	var result core.SearchResult
	var sTime time.Time
	var responseBytes []byte
	type TType struct {
		Updated_Ad       []time.Time `json:"_updated_ad,omitempty"`
		Updated_Campaign []time.Time `json:"_updated_campaign,omitempty"`
	}
	var lastUpdated TType

	q := `{ "size":1, "fields":["` + col + `"], "query" : { "match_all":{} } , "sort" : [ { "` + col + `" : { "order":"desc" } } ] }`

	result, err = core.SearchRequest("campaigns", "ads", nil, q)
	if &result != nil && &result.Hits != nil && len(result.Hits.Hits) > 0 && result.Hits.Hits[0].Fields != nil {
		if responseBytes, err = result.Hits.Hits[0].Fields.MarshalJSON(); err != nil {
			log.Println("Unable to Marshall from ES. Using default date: ", time.Time{})
			return sTime
		} else {
			if err = json.Unmarshal(responseBytes, &lastUpdated); err != nil {
				log.Println("Unable to Unmarshall last updated from ES. Using default date - ", err)
				return sTime
			} else {
				if len(lastUpdated.Updated_Ad) > 0 {
					sTime = lastUpdated.Updated_Ad[0]
				} else if len(lastUpdated.Updated_Campaign) > 0 {
					sTime = lastUpdated.Updated_Campaign[0]
				}
			}

		}
	}

	return sTime
}

func getLastUpdated(mongoSession *mgo.Session, coll string, last time.Time) ([]de.Ad, error) {

	var (
		err  error
		ad_s []de.Ad
		ad   de.Ad
	)
	iter := RunQuery(mongoSession, coll, last)
	for {
		if iter.Next(&ad) {

			ad_s = append(ad_s, ad)
		} else {
			break
		}
	}

	err = iter.Err()
	if err != nil {
		log.Println("Error getting response from mongo db")
		panic(err)
	}

	return ad_s, err
}

// DE servers run a process every X seconds to query data from campaign db and
// update themselves with new/update campaign information from campaign db.

func main() {
	var url string

	flag.StringVar(&url, "url", "mongodb://localhost/pixelle", "mongodb dial url")
	flag.Parse()

	api.Domain = "localhost"
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
	// Perform 10 concurrent queries against the database.
	var last time.Time
	for {
		last = getESLastUpdated("_updated_ad")
		log.Println("Last ad updated timestamp:", last.Format(time.RFC3339))

		if updatedAds, err = getLastUpdated(mongoSession, "ads", last); err != nil {
			log.Println("Error while getting latest ads data from mongodb. Last updated timestamp on ES: ", last)
		}
		log.Println("1. Num ads to update", len(updatedAds))
		for _, value := range updatedAds {
			indexAdFromAdData(&value)
		}
		time.Sleep(10 * time.Second)
		last = getESLastUpdated("_updated_campaign")
		log.Println("Last campaign updated timestamp:", last.Format(time.RFC3339))
		if updatedCampaigns, err = getLastUpdated(mongoSession, "campaigns", last); err != nil {
			log.Println("Error while getting latest campaigns data from mongodb. Last updated timestamp on ES: ", last)
		}
		log.Println("2. Num campaigns to update", len(updatedCampaigns))
		for _, v := range updatedCampaigns {
			// here the v.AdId is really the campaign Id
			if adIds, err := de.GetAdIdsByCampaign(v.AdId.Hex()); err != nil {
				panic(err.Error())
			} else {
				for _, val := range adIds {
					if err = indexAdFromCampaignData(&v, val); err != nil {
						panic(err.Error())
					}
				}
			}
		}

		log.Println("All Queries Completed")
		time.Sleep(1 * time.Minute)
	}
}
func indexAdFromCampaignData(c *de.Ad, ad_id string) *de.DeError {
	var ad de.Ad

	ad.AdId = bson.ObjectIdHex(ad_id)
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
func indexAdFromAdData(addb *de.Ad) *de.DeError {
	return de.IndexAd(addb)

}
