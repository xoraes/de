package delib

import (
	"labix.org/v2/mgo"
	"time"
	"labix.org/v2/mgo/bson"
	"log"
)

func GetLastUpdated(mongoSession *mgo.Session, coll string, last time.Time) ([]Ad, error) {
	var (
		err  error
		ad_s []Ad
		ad   Ad
	)
	// Request a socket connection from the session to process our query.
	// Close the session when the goroutine exits and put the connection back
	// into the pool.
	sessionCopy := mongoSession.Copy()
	defer sessionCopy.Close()

	// Get a collection to execute the query against.
	// DB returns a value representing the named database. If name
	// is empty, the database name provided in the dialed URL is
	// used instead. If that is also empty, "test" is used as a
	// fallback in a way equivalent to the mongo shell.
	collection := sessionCopy.DB("").C(coll)
	iter := collection.Find(bson.M{"_updated": bson.M{"$gt": last}}).Iter()

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

