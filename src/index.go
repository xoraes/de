package main

import (
	"fmt"
	"github.com/mattbaird/elastigo/api"
	"time"
)

func main() {
	api.Domain = "localhost"
	api.Port = "9200"

	var err error
	var a1 Ad
	a1.AdId = "1"
	a1.Description = "this is the decription1"
	a1.ExcludedLocations = []string{"Paris", "New York"}
	a1.AdFormats = []int{1, 3}
	a1.VideoUrl = "http://www.youtube.com/cat-video"
	a1.Title = "US and France Ad"
	a1.Locations = []string{"12345", "fr", "us", "ca:345", "fr:75017", "dma:123"}
	a1.Languages = []string{"en", "fr"}
	a1.TacticId = "1234"
	a1.ThumbnailUrl = "http://www.youtube.com/thumbnail-url"
	a1.CampaignId = a1.AdId
	//a1.Updated, err = time.Parse(time.RFC3339,time.Now().Format(time.RFC3339))
	a1.Updated,_ = time.Parse(time.RFC3339,"2013-07-08T00:25:11Z")
	if err != nil {
		panic(err)
	}

	var a2 Ad
	a2.TacticId = "1234"
	a2.Description = "this is the decription2"
	a2.ExcludedLocations = []string{"Mountain View", "Campbell"}
	a2.AdFormats = []int{1, 3}
	a2.VideoUrl = "http://www.youtube.com/cat-video"
	a2.Title = "California only Ad"
	a2.Locations = []string{"fr:1123"}
	a2.Languages = []string{"fr"}
	a2.AdId = "2"
	a2.CampaignId = a2.AdId
	a2.ThumbnailUrl = "http://www.youtube.com/thumbnail-url"
	a2.AdFormats = []int{1, 3}
	a2.Updated,_ = time.Parse(time.RFC3339,"2013-07-08T00:30:11Z")

	var a3 Ad
	a3.TacticId = "1234"
	a3.Description = "this is the decription1"
	a3.ExcludedLocations = []string{"Paris"}
	a3.VideoUrl = "http://www.youtube.com/cat-video"
	a3.Title = "france local Ad"
	a3.Locations = []string{"fr:1234", "fr"}
	a3.Languages = []string{"en", "fr"}
	a3.AdId = "3"
	a3.CampaignId = a3.AdId
	a3.ThumbnailUrl = "http://www.youtube.com/thumbnail-url"
	a3.IsPaused = false
	a3.GoalReached = true
	a3.AdFormats = []int{1, 3}
	a3.Updated,_ = time.Parse(time.RFC3339,"2013-07-08T00:28:11Z")

	var a4 Ad
	a4.TacticId = "1234"
	a4.Description = "this is the decription2"
	a4.ExcludedLocations = []string{"Paris"}
	a4.AdFormats = []int{1, 3}
	a4.VideoUrl = "http://www.youtube.com/cat-video"
	a4.Title = "france local Ad"
	a4.Locations = []string{"fr:1234"}
	a4.Languages = []string{"fr"}
	a4.AdId = "4"
	a4.CampaignId = a4.AdId
	a4.ThumbnailUrl = "http://www.youtube.com/thumbnail-url"
	a4.Updated,_ = time.Parse(time.RFC3339,"2013-07-08T00:27:11Z")

	var a5 Ad
	a5.GoalReached = true
	a5.Locations = []string{"de"}
	a5.Languages = []string{"de"}
	a5.AdFormats = []int{1, 3}
	a5.Updated,_ = time.Parse(time.RFC3339,"2013-07-08T00:31:11Z")
	a5.AdId = "5"

	var r api.BaseResponse
	if r, err = a1.indexAd(); err != nil {
		panic("Error writing sample data to localhost es")
	} else {
		fmt.Println(r)
	}
	if r, err = a2.indexAd(); err != nil {
		panic("Error writing sample data to localhost es")
	} else {
		fmt.Println(r)
	}
	if r, err = a3.indexAd(); err != nil {
		panic("Error writing sample data to localhost es")
	} else {
		fmt.Println(r)
	}
	if r, err = a4.indexAd(); err != nil {
		panic("Error writing sample data to localhost es")
	} else {
		fmt.Println(r)
	}
	if r, err = a5.indexAd(); err != nil {
		panic("Error writing sample data to localhost es")
	} else {
		fmt.Println(r)
	}

}
