package main

import (
	"encoding/json"
	"github.com/golang/glog"
	"github.com/mattbaird/elastigo/api"
	"github.com/mattbaird/elastigo/core"
	"io"
	"net/http"
	"strconv"
	"strings"
	"fmt"
)

func indexAd(ad *Ad) *DeError {
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

	if jsonBytes, serr := json.Marshal(ad); serr != nil {
		return NewError(500, serr)
	} else if _, serr := core.Index("campaigns", "ads", ad.AdId.Hex(), nil, jsonBytes); serr != nil {
		glog.Error(serr)
		return NewError(500, serr)
	}
	return nil
}
func updateAd(ad *Ad) *DeError {

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
	if jsonBytes, serr := json.Marshal(ad); serr != nil {
		glog.Error(serr)
		return NewError(500, serr)
	} else {
		fmt.Println("Updating ad w/campaign data:",string(jsonBytes))
		if _, serr := core.UpdateWithPartialDoc("campaigns", "ads", ad.AdId.Hex(), nil, string(jsonBytes), false); serr != nil {
			glog.Error(serr)
			return NewError(500, serr)
		}
	}
	return nil
}
func processQuery(req *http.Request) ([]byte, *DeError) {
	var (
		byteArray []byte
		err       error
		derr      *DeError
		ads       []Ad
		sq        SearchQuery
		positions int
	)
	//decode raw byte to struct for SearchQuery
	decoder := json.NewDecoder(req.Body)
	//if a req body is null, we ignore the decoder err and proceed. Note this
	//means we will return ad(s) when the request body is empty
	if err = decoder.Decode(&sq); err != nil && err != io.EOF {
		return nil, NewError(500, err)
	}
	//get the positions needed
	if adcount := req.URL.Query().Get("positions"); adcount == "" {
		positions = 1
	} else if positions, err = strconv.Atoi(adcount); err != nil {
		positions = 1
	}
	//send query to es
	if ads, derr = queryES(positions, sq); derr != nil {
		return nil, derr
	} else if byteArray, err = json.MarshalIndent(ads, "", "    "); err != nil {
		return nil, NewError(500, err)
	}
	return byteArray, nil
}
func queryES(positions int, sq SearchQuery) ([]Ad, *DeError) {
	var (
		byteArray []byte
		err       error
		ads       []Ad
		sresult   core.SearchResult
	)
	//run the actual query using elastigo
	if sresult, err = core.SearchRequest("campaigns", "ads", nil, createESQueryString(positions, sq)); err != nil {
		return nil, NewError(500, err)
		//if any results are obtained
	} else if &sresult != nil && sresult.Hits.Total > 0 {

		ads = make([]Ad, len(sresult.Hits.Hits))
		for i, hit := range sresult.Hits.Hits {
			if byteArray, err = hit.Source.MarshalJSON(); err != nil {
				return nil, NewError(500, err)
			} else if err = json.Unmarshal(byteArray, &ads[i]); err != nil {
				return nil, NewError(500, err)
			}
		}
		//send this when empty results are obtained
	} else {
		//A degradation logic could be implemented here instead of sending error response
		return nil, &DeError{Code: http.StatusOK, Msg: "No ads were found matching the target criteria"}
	}
	glog.Info(`{"took_ms":`, sresult.Took, `,"timedout":`, sresult.TimedOut, `,"hitct":`, sresult.Hits.Total, "}")
	return ads, nil
}

func createESQueryString(numPositions int, sq SearchQuery) string {
	var (
		err error
		loc []byte
		q   string
	)

	q = `{"size":`
	q += strconv.Itoa(numPositions) + ","
	q += `"query": {
      "function_score": {
        "query": {
            "filtered": {
                "filter":   {`
	delim := ""
	useMustFilter := (sq.Locations != nil && len(sq.Locations) > 0) || (sq.Languages != nil && len(sq.Languages) > 0) || sq.AdFormat > 0
	//useMustFilter := true

	q += `"bool":{`
	if useMustFilter {
		q += `"must":[`
		if sq.Locations != nil && len(sq.Locations) > 0 {
			loc, err = json.Marshal(sq.Locations)
			if err == nil {
				q += `{ "query":  {"terms": { "locations":` + strings.ToLower(string(loc)) + `}}}`
				delim = ","
			}
		}
		if sq.Languages != nil && len(sq.Languages) > 0 {
			var lang []byte
			lang, err = json.Marshal(sq.Languages)
			if err == nil {
				q += delim + `{ "query":  {"terms": { "languages":` + strings.ToLower(string(lang)) + `}}}`
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
	if len(sq.Locations) > 0 && err == nil {
		q += `,{ "query":  {"terms": { "excluded_locations":` + strings.ToLower(string(loc)) + `}}}`
		delim = ","
	}
	q += `]}}}},"random_score": {}}}}`

	glog.Info("==== Generated ES query ====>")
	glog.Info(q)
	glog.Info("=============================")
	return q
}

func getAdById(id string) ([]byte, *DeError) {
	var (
		qres          api.BaseResponse
		serr          error
		br            []byte
		ad            *Ad
		responseBytes []byte
	)
	if qres, serr = core.Get("campaigns", "ads", id, nil); serr != nil {
		return nil, NewError(500, "Error talking to ES")
	} else if !qres.Found {
		return nil, NewError(400, "Could not find id in ES: "+id)
	} else if br, serr = json.Marshal(qres.Source); serr != nil {
		return nil, NewError(500, serr)
	} else if serr = json.Unmarshal(br, ad); serr != nil {
		return nil, NewError(500, serr)
	} else if responseBytes, serr = json.Marshal(ad); serr != nil {
		return nil, NewError(500, serr)
	}
	return responseBytes, nil
}
func deleteAdById(id string) *DeError {
	glog.Info("ad to delete: " + id)
	if qres, err := core.Delete("campaigns", "ads", id, nil); err != nil {
		return NewError(http.StatusInternalServerError, err)
	} else {
		if !qres.Found {
			errstr := "ad id " + id + " does not exist"
			return NewError(http.StatusInternalServerError, errstr)
		}
	}
	return nil
}

func postAdToES(req *http.Request) ([]byte, *DeError) {
	var (
		ad   Ad
		err  *DeError
		serr error
	)
	decoder := json.NewDecoder(req.Body)
	if serr = decoder.Decode(&ad); serr != nil {
		return nil, NewError(500, serr)
	} else if ad.AdId == "" {
		return nil, NewError(400, "no ad_id found")
	} else if err = indexAd(&ad); err != nil {
		return nil, NewError(500, err)
	} else {
		//success
		return nil, nil
	}
}

func getAdIdsByCampaign(cid string) ([]string, *DeError) {
	var (
		serr    error
		sresult core.SearchResult
		ids     []string
	)
	q := `{"filter": {"bool": {"must": [{"term": {"campaign":"` + cid + `"}}]}},"fields": []}`
	if sresult, serr = core.SearchRequest("campaigns", "ads", nil, q); serr != nil {
		return nil, NewError(500, serr)
	}
	for _, v := range sresult.Hits.Hits {
		ids = append(ids, v.Id)
	}
	return ids, nil
}
