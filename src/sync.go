package main

import (
	"fmt"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"sync"
	"time"
	"encoding/json"
	"github.com/mattbaird/elastigo/api"
	"github.com/mattbaird/elastigo/core"

	"github.com/golang/glog"
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


	iter = collection.Find(bson.M{"_updated": bson.M{"$gt": last.Add(-time.Minute*2)}}).Iter()
	return iter
}

func getESLastUpdatedAd() (time.Time) {
	var err error
	var result core.SearchResult
	var sTime time.Time
	var responseBytes []byte
	var qr SearchResponse
	api.Domain = "localhost"
	api.Port = "9200"

	q := `{ "size":1, "query" : { "match_all":{} } , "sort" : [ { "updated_at" : { "order":"desc" } } ] }`

	//TODO :retry logic goes here - implement a doCommand wrapper here
	//TODO: The wrapper should also track total response latency

	result, err = core.SearchRequest("campaigns", "ads", nil, q)
	if &result != nil && &result.Hits != nil && len(result.Hits.Hits) > 0 {
		if responseBytes, err = json.Marshal(result.Hits.Hits[0].Source); err != nil {
			return sTime
		} else {
			if err = json.Unmarshal(responseBytes,&qr); err != nil {
				return sTime
			} else {
				sTime = qr.UpdatedTS
			}
		}
	}
	return sTime
}

func getLastUpdatedCampaigns(waitGroup *sync.WaitGroup,mongoSession *mgo.Session, coll string, last time.Time) ([]CampaignDb,error){

	defer waitGroup.Done()
	var (
		err error
		results []CampaignDb
		result  CampaignDb
	)
	iter := RunQuery(mongoSession,coll,last)
	for {
		if iter.Next(&result) {
			results = append(results, result)
		} else {
			break
		}
	}
	err = iter.Err()
	if err != nil {
		glog.Errorln("Error getting response from mongo db")
	}

	return results,err
}
// DE servers run a process every X seconds to query data from campaign db and
// update themselves with new/update campaign information from campaign db.

func main() {

	fmt.Println(getESLastUpdatedAd())
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

	// Create a wait group to manage the goroutines.
	var waitGroup sync.WaitGroup

	// Perform 10 concurrent queries against the database.
	waitGroup.Add(1)
	last := getESLastUpdatedAd()
	var updatedCampaigns []CampaignDb

	go func (){
		if updatedCampaigns,err = getLastUpdatedCampaigns(&waitGroup, mongoSession, "campaigns", last); err != nil {
			glog.Error("Could not update DE with latest campaigns data from ", last)
		}
	} ()


	//go getLastUpdatedAds(&waitGroup, mongoSession, "campaigns", last)

	// Wait for all the queries to complete.
	waitGroup.Wait()

	for i,v := range updatedCampaigns {
		fmt.Println(i,v)
	}
	log.Println("All Queries Completed")

}
