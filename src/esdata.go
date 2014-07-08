package main

import (
	"encoding/json"
	"fmt"
	"github.com/mattbaird/elastigo/api"
	"github.com/mattbaird/elastigo/core"
	"strconv"
	"time"
	"flag"
)

type Ad struct {
	AdId              string   `json:"ad_id,omitempty"`
	CampaignId        string   `json:"campaign_id,omitempty"`
	Languages         []string `json:"languages,omitempty"`
	Locations         []string `json:"locations,omitempty"`
	ExcludedLocations []string `json:"excluded_locations,omitempty"`
	AdFormats         []string `json:"ad_formats,omitempty"`
	Account           string   `json:"account_id,omitempty"`
	VideoUrl          string   `json:"video_url,omitempty"`
	ThumbnailUrl      string   `json:"thumbnail_url,omitempty"`
	Description       string   `json:"description,omitempty"`
	Title             string   `json:"title,omitempty"`
	TacticId          string   `json:"tactic_id,omitempty"`
	AuthorId          string   `json:"author_id,omitempty"`
	Author            string   `json:"author,omitempty"`
	IsPaused          bool   `json:"is_paused,omitempty"`
	GoalReached		  bool     `json:"goal_reached,omitempty"`
}

func IndexSampleAdsBulk() {
	var count int

	const (
		START   int = 1000
		NUMRECS int = 5
	)
	indexer := core.NewBulkIndexerErrors(100, 60)
	done := make(chan bool)
	indexer.Run(done)

	go func() {
		for errBuf := range indexer.ErrorChannel {
			// just blissfully print errors forever
			fmt.Println(errBuf.Err)
		}
	}()
	for count = START; count < NUMRECS+START; count++ {

		var a1 Ad
		a1.Description = "this is the decription1"
		a1.ExcludedLocations = []string{"Paris", "New York"}
		a1.AdFormats = []string{"1", "3"}
		a1.VideoUrl = "http://www.youtube.com/cat-video"
		a1.Title = "US and France Ad"
		a1.Locations = []string{"12345", "fr", "us", "ca:345", "fr:75017", "dma:123"}
		a1.Languages = []string{"en", "fr"}
		a1.AdId = strconv.Itoa(count + 1)
		a1.TacticId = "1234"
		a1.ThumbnailUrl = "http://www.youtube.com/thumbnail-url"
		a1.Author = "Author Name"
		a1.AuthorId = "11111"
		a1.CampaignId = a1.AdId


		var a2 Ad
		a2.TacticId = "1234"
		a2.Description = "this is the decription2"
		a2.ExcludedLocations = []string{"Mountain View", "Campbell"}
		a2.AdFormats = []string{"1", "3"}
		a2.VideoUrl = "http://www.youtube.com/cat-video"
		a2.Title = "California only Ad"
		a2.Locations = []string{"fr:1123"}
		a2.Languages = []string{"fr"}
		a2.AdId = strconv.Itoa(count + NUMRECS + 1)
		a2.CampaignId = a2.AdId
		a2.ThumbnailUrl = "http://www.youtube.com/thumbnail-url"
		a2.Author = "Author Name"
		a2.AuthorId = "11111"
		a2.AdFormats = []string{"1", "3"}

		var a3 Ad
		a3.TacticId = "1234"
		a3.Description = "this is the decription1"
		a3.ExcludedLocations = []string{"Paris"}
		a3.VideoUrl = "http://www.youtube.com/cat-video"
		a3.Title = "france local Ad"
		a3.Locations = []string{"fr:1234", "fr"}
		a3.Languages = []string{"en", "fr"}
		a3.AdId = strconv.Itoa(count + NUMRECS + 1)
		a3.CampaignId = a3.AdId
		a3.ThumbnailUrl = "http://www.youtube.com/thumbnail-url"
		a3.Author = "Author Name"
		a3.AuthorId = "11111"
		a3.IsPaused = false
		a3.GoalReached = true
		a3.AdFormats = []string{"1", "3"}

		var a4 Ad
		a4.TacticId = "1234"
		a4.Description = "this is the decription2"
		a4.ExcludedLocations = []string{"Paris"}
		a4.AdFormats = []string{"1", "3"}
		a4.VideoUrl = "http://www.youtube.com/cat-video"
		a4.Title = "france local Ad"
		a4.Locations = []string{"fr:1234"}
		a4.Languages = []string{"fr"}
		a4.AdId = strconv.Itoa(count + NUMRECS + 1)
		a4.CampaignId = a4.AdId
		a4.ThumbnailUrl = "http://www.youtube.com/thumbnail-url"
		a4.Author = "Author Name"
		a4.AuthorId = "11111"

		var a5 Ad
		a5.GoalReached=true
		a5.Locations = []string{"de"}
		a5.Languages = []string{"de"}
		a5.AdFormats = []string{"1", "3"}

		type ajson struct {
			jsonBytes []byte
			err error
		}
		var bArray make([]byte,10)

		bArray[0],_ = json.Marshal(a1)
		bArray[1],_ = json.Marshal(a2)
		bArray[2],_ = json.Marshal(a3)
		bArray[3],_ = json.Marshal(a4)
		bArray[4],_ = json.Marshal(a5)
		for index, value := range bArray {
			indexer.Index("campaigns", "ads", strconv.Itoa(count+(index*NUMRECS)+1), "", nil, value, false)
		}
	}
	done <- true
	time.Sleep(time.Second * time.Duration(8))
}

func main() {
	var eshost string
	flag.StringVar(&eshost, "eshost", "localhost", "elasticsearch host ip or hostname")
	flag.Parse()
	api.Domain = eshost
	api.Port = "9200"

	IndexSampleAdsBulk()
}
