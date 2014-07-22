package main

import (
	"encoding/json"
	"fmt"
	"github.com/mattbaird/elastigo/api"
	"github.com/mattbaird/elastigo/core"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"time"
)

const (
	MongoDBHosts = "localhost"
	PxDb         = "pixelle"
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

func getESLastUpdatedAdTime() time.Time {
	var err error
	var result core.SearchResult
	var sTime time.Time
	var responseBytes []byte
	type TType struct {
		Updated []time.Time `json:"_updated,omitempty"`
	}
	var lastUpdated TType

	q := `{ "size":1, "fields":["_updated"], "query" : { "match_all":{} } , "sort" : [ { "_updated" : { "order":"desc" } } ] }`
	result, err = core.SearchRequest("campaigns", "ads", nil, q)
	if &result != nil && &result.Hits != nil && len(result.Hits.Hits) > 0 {
		if responseBytes, err = result.Hits.Hits[0].Fields.MarshalJSON(); err != nil {
			log.Println("Unable to Marshall from ES. Using default date: ", time.Time{})
			return sTime
		} else {
			if err = json.Unmarshal(responseBytes, &lastUpdated); err != nil {
				log.Println("Unable to Unmarshall last updated from ES. Using default date - ", err)
				return sTime
			} else {
				sTime = lastUpdated.Updated[0]

			}
		}
	}
	return sTime
}

func getLastUpdatedCampaigns(mongoSession *mgo.Session, coll string, last time.Time) ([]CampaignDb, error) {

	var (
		err        error
		campaign_s []CampaignDb
		campaign   CampaignDb
	)
	iter := RunQuery(mongoSession, coll, last)

	for {
		if iter.Next(&campaign) {
			campaign_s = append(campaign_s, campaign)
		} else {
			break
		}
	}
	err = iter.Err()
	if err != nil {
		log.Println("Error getting response from mongo db")
	}

	return campaign_s, err
}
func getLastUpdatedAds(mongoSession *mgo.Session, coll string, last time.Time) ([]Ad, error) {

	var (
		err  error
		ad_s []Ad
		ad   Ad
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
	api.Domain = "localhost"
	api.Port = "9200"
	// We need this object to establish a session to our MongoDB.
	mongoDBDialInfo := &mgo.DialInfo{
		Addrs:   []string{MongoDBHosts},
		Timeout: 60 * time.Second,
	}

	// Create a session which maintains a pool of socket connections
	// to our MongoDB.
	mongoSession, err := mgo.DialWithInfo(mongoDBDialInfo)
	if err != nil {
		log.Fatalf("CreateSession: %s\n", err)
	}

	// Reads may not be entirely up-to-date, but they will always see the
	// history of changes moving forward, the data read will be consistent
	// across sequential queries in the same session, and modifications made
	// within the session will be observed in following queries (read-your-writes).
	// http://godoc.org/labix.org/v2/mgo#Session.SetMode
	mongoSession.SetMode(mgo.Monotonic, true)

	// Perform 10 concurrent queries against the database.

	last := getESLastUpdatedAdTime()
	log.Println("Last updated timestamp:", last.Format(time.RFC3339))
	var updatedCampaigns []CampaignDb
	var updatedAds []Ad


	if updatedAds, err = getLastUpdatedAds(mongoSession, "ads", last); err != nil {
		log.Println("Error while getting latest ads data from mongodb. Last updated timestamp on ES: ", last)
	}
	for _,value := range updatedAds {
		indexAdFromAdData(&value)
	}
	if updatedCampaigns, err = getLastUpdatedCampaigns(mongoSession, "campaigns", last); err != nil {
		log.Println("Error while getting latest campaigns data from mongodb. Last updated timestamp on ES: ", last)
	}
	log.Println("Num campaigns to update", len(updatedCampaigns))
	for _, v := range updatedCampaigns {
		if adIds, err := getAdIdsByCampaign(v.Id.Hex()); err != nil {
			panic(err.Error())
		} else {
			for _, val := range adIds {
				fmt.Println("ad id:" + val)
				if err = indexAdFromCampaignData(&v, val); err != nil {
					panic(err.Error())
				}
			}
		}
	}

	log.Println("All Queries Completed")

}
func indexAdFromCampaignData(c *CampaignDb, ad_id string) *DeError {
	var ad Ad
	ad.AdId = bson.ObjectIdHex(ad_id)
	ad.CampaignId = c.Id
	ad.Updated = c.Updated
	ad.Locations = c.Locations
	ad.Account = c.Account
	ad.ExcludedLocations = c.ExcludedLocations
	ad.Paused = c.Paused
	ad.GoalViews = c.GoalViews
	ad.GoalPeriod = c.GoalPeriod
	log.Println("Indexing campign data to ads", ad)
	return updateAd(&ad)
}
func indexAdFromAdData(addb *Ad) *DeError {
	return indexAd(addb)

}
