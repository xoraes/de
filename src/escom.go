package main

import (
	"encoding/json"
	"github.com/golang/glog"
	"github.com/mattbaird/elastigo/api"
	"github.com/mattbaird/elastigo/core"
	"strconv"
	"strings"
)

func (ad Ad) indexAd() (response api.BaseResponse, err error) {
	var index int
	for index, _ = range ad.Languages {
		ad.Languages[index] = strings.ToLower(ad.Languages[index])
	}
	for index, _ = range ad.Locations {
		ad.Locations[index] = strings.ToLower(ad.Locations[index])
	}
	for index, _ = range ad.ExcludedLocations {
		ad.ExcludedLocations[index] = strings.ToLower(ad.ExcludedLocations[index])
	}

	jsonBytes, err := json.Marshal(ad)
	//TODO :retry logic goes here - implement a doCommand wrapper here
	if response, err = core.Index("campaigns", "ads", ad.AdId, nil, jsonBytes); err != nil {
		glog.Error(err)
	}
	return response, err
}
func (sq SearchQuery) queryES() (core.SearchResult, error) {
	//TODO :retry logic goes here - implement a doCommand wrapper here
	//TODO: The wrapper should also track total response latency

	sresult, err := core.SearchRequest("campaigns", "ads", nil, sq.createESQueryString())
	if &sresult != nil {
		//TODO: metric data - send to new relic
		glog.Info(`{"took_ms":`, sresult.Took, `,"timedout":`, sresult.TimedOut, `,"hitct":`, sresult.Hits.Total, "}")
	}
	return sresult, err
}

func (sq SearchQuery) createESQueryString() (q string) {
	var err, locerr error
	var loc []byte
	q = `{"from": 0,
    "size": 1,
    "query": {
      "function_score": {
        "query": {
            "filtered": {
                "filter":   {`
	delim := ""
	useMustFilter := len(sq.Locations) > 0 || len(sq.Languages) > 0 || sq.AdFormat > 0

	q += `"bool":{`
	if useMustFilter {
		q += `"must":[`
		if len(sq.Locations) > 0 {
			loc, locerr = json.Marshal(sq.Locations)
			if err == nil {
				q += `{ "query":  {"terms": { "locations":` + strings.ToLower(string(loc)) + `}}}`
				delim = ","
			}
		}
		if len(sq.Languages) > 0 {
			var lang []byte
			lang, err = json.Marshal(sq.Languages)
			if err == nil {
				q += delim + `{ "query":  {"terms": { "languages":` + string(lang) + `}}}`
				delim = ","
			}
		}

		if sq.AdFormat > 0 { //ad format values should be greater than 0

			q += delim + `{ "query":  {"term": { "ad_formats":` + strconv.Itoa(sq.AdFormat) + `}}}`
			delim = ","
		}
		q += `]`
	}
	q += delim + `"must_not":[`
	//is_paused and goal_reached are separate fields
	//so they can be changed independently
	q += `{ "query":  {"term": { "is_paused": "true"}}}`
	q += `,{ "query":  {"term": { "goal_reached": "true"}}}`
	if len(sq.Locations) > 0 && locerr == nil {
		q += `,{ "query":  {"terms": { "excluded_locations":` + string(loc) + `}}}`
		delim = ","
	}
	q += `]}}}},"random_score": {}}}}`

	glog.Info("==== Generated ES query ====>")
	glog.Info(q)
	glog.Info("=============================")
	return q
}
