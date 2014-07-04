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
	AdFormats         []int    `json:"ad_formats,omitempty"`
	Account           string   `json:"account_id,omitempty"`
	VideoUrl          string   `json:"video_url,omitempty"`
	ThumbnailUrl      string   `json:"thumbnail_url,omitempty"`
	Description       string   `json:"description,omitempty"`
	Title             string   `json:"title,omitempty"`
	TacticId             string   `json:"tactic_id,omitempty"`
}

func IndexSampleAdsBulk() {
	var count int

	const (
		START   int = 1000
		NUMRECS int = 20
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
		a1.AdFormats = []int{1, 3}
		a1.VideoUrl = "http://www.youtube.com/cat-video"
		a1.Title = "US and France Ad"
		a1.Locations = []string{"12345", "fr", "us", "ca:345", "fr:75017", "dma:123"}
		a1.Languages = []string{"en", "fr"}
		a1.AdId = strconv.Itoa(count + 1)
		a1.TacticId = "1234"
		a1.CampaignId = a1.AdId


		var a2 Ad
		a2.TacticId = "1234"
		a2.Description = "this is the decription2"
		a2.ExcludedLocations = []string{"Mountain View", "Campbell"}
		a2.AdFormats = []int{1, 3}
		a2.VideoUrl = "http://www.youtube.com/cat-video"
		a2.Title = "California only Ad"
		a2.Locations = []string{"fr:1123"}
		a2.Languages = []string{"fr"}
		a2.AdId = strconv.Itoa(count + NUMRECS + 1)
		a2.CampaignId = a2.AdId

		var a3 Ad
		a3.TacticId = "1234"
		a3.Description = "this is the decription1"
		a3.ExcludedLocations = []string{"Paris"}
		a3.AdFormats = []int{1, 3}
		a3.VideoUrl = "http://www.youtube.com/cat-video"
		a3.Title = "france local Ad"
		a3.Locations = []string{"fr:1234", "fr"}
		a3.Languages = []string{"en", "fr"}
		a3.AdId = strconv.Itoa(count + NUMRECS + 1)
		a3.CampaignId = a3.AdId

		var a4 Ad
		a4.TacticId = "1234"
		a4.Description = "this is the decription2"
		a4.ExcludedLocations = []string{"Paris"}
		a4.AdFormats = []int{1, 3}
		a4.VideoUrl = "http://www.youtube.com/cat-video"
		a4.Title = "france local Ad"
		a4.Locations = []string{"fr:1234"}
		a4.Languages = []string{"fr"}
		a4.AdId = strconv.Itoa(count + NUMRECS + 1)
		a4.CampaignId = a4.AdId

		var a1Bytes, a2Bytes, a3Bytes, a4Bytes []byte
		var err1, err2,err3,err4 error
		a1Bytes, err1 = json.Marshal(a1)
		a2Bytes, err2 = json.Marshal(a2)
		a3Bytes, err3 = json.Marshal(a3)
		a4Bytes, err4 = json.Marshal(a4)

		if err1 !=nil || err2 !=nil || err3 !=nil || err4 !=nil {
			fmt.Println("error:", err1, err2, err3, err4)
		}
		//if errFrOnly != nil || err != nil {
		//	fmt.Println("error:", err, errFrOnly)
		//	panic(err)
		//}

		fmt.Println(string(a1Bytes))
		fmt.Println(string(a2Bytes))
		fmt.Println(string(a3Bytes))
		fmt.Println(string(a4Bytes))

		indexer.Index("campaigns", "ads", strconv.Itoa(count+1), "", nil, a1Bytes, false)
		indexer.Index("campaigns", "ads", strconv.Itoa(count+NUMRECS+1), "", nil, a2Bytes, false)
		indexer.Index("campaigns", "ads", strconv.Itoa(count+2*NUMRECS+1), "", nil, a3Bytes, false)
		indexer.Index("campaigns", "ads", strconv.Itoa(count+3*NUMRECS+1), "", nil, a4Bytes, false)
	}
	done <- true
	time.Sleep(time.Second * time.Duration(8))
}

func main() {
	var eshost string
	flag.StringVar(&eshost, "eshost", "es.pxlad.in", "elasticsearch host ip or hostname")
	flag.Parse()
	api.Domain = eshost
	api.Port = "9200"

	IndexSampleAdsBulk()
}
