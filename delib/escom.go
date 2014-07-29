package delib

import (
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	elastigo "github.com/mattbaird/elastigo/lib"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var c *elastigo.Conn

func init() {
	c = elastigo.NewConn()
}

func IndexAd(ad *Ad) *DeError {
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
	ad.Status = strings.ToLower(ad.Status)

	if jsonBytes, serr := json.Marshal(ad); serr != nil {
		return NewError(500, serr)
	} else if _, serr := c.Index("campaigns", "ads", ad.Id.Hex(), nil, jsonBytes); serr != nil {
		glog.Error(serr)
		return NewError(500, serr)
	}
	return nil
}
func UpdateAd(ad *Ad) *DeError {

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
	ad.Status = strings.ToLower(ad.Status)

	if jsonBytes, serr := json.Marshal(ad); serr != nil {
		glog.Error(serr)
		return NewError(500, serr)
	} else {
		fmt.Println("Updating ad on ES:", string(jsonBytes))
		if _, serr := c.UpdateWithPartialDoc("campaigns", "ads", ad.Id.Hex(), nil, string(jsonBytes), true); serr != nil {
			glog.Error(serr)
			return NewError(500, serr)
		}
	}
	return nil
}
func ProcessQuery(req *http.Request) ([]byte, *DeError) {
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
	}

	sqr := &SearchQueryResponse{AdUnits: ads}
	if byteArray, err = json.MarshalIndent(sqr, "", "    "); err != nil {
		return nil, NewError(500, err)
	}
	return byteArray, nil
}
func queryES(positions int, sq SearchQuery) ([]Ad, *DeError) {
	var (
		byteArray []byte
		err       error
		ads       []Ad
		sresult   elastigo.SearchResult
	)
	//run the actual query using elastigo
	if sresult, err = c.Search("campaigns", "ads", nil, createESQueryString(positions, sq)); err != nil {
		return nil, NewError(500, err)
		//if any results are obtained
	} else if &sresult != nil && sresult.Hits.Total > 0 {

		ads = make([]Ad, len(sresult.Hits.Hits))
		for i, hit := range sresult.Hits.Hits {
			if byteArray, err = json.Marshal(hit.Source); err != nil {
				return nil, NewError(500, err)
			} else if err = json.Unmarshal(byteArray, &ads[i]); err != nil {
				return nil, NewError(500, err)
			}
		}
		//send this when empty results are obtained
	} else {
		target, _ := json.Marshal(&sq)
		//A degradation logic could be implemented here instead of sending error response
		return nil, NewError(200, "No ads were found matching the target criteria - "+string(target))
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
	q = `{"_source":
			{
			"include": ["_id","campaign","title","description","account","tactic","video_url","thumbnail_url","channel","channel_url","duration"],
			"exclude": ["paused_ad","paused_campaign"]
			},`
	q += `"size":`
	q += strconv.Itoa(numPositions) + ","
	q += `"query": {
      "function_score": {
        "query": {
            "filtered": {
                "filter":   {`
	delim := ""
	//useMustFilter := (sq.Locations != nil && len(sq.Locations) > 0) || (sq.Languages != nil && len(sq.Languages) > 0) || sq.AdFormat > 0
	useMustFilter := true

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
		q += delim + `{ "query":  {"term": { "status": "approved"}}}`
		delim = ","
		q += `]`
	}

	q += delim + `"must_not":[`
	q += `{ "query":  {"term": { "paused_campaign": true}}}`
	q += `,{ "query":  {"term": { "paused_ad": true}}}`

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

func GetAdById(id string) ([]byte, *DeError) {
	var (
		qres          elastigo.BaseResponse
		serr          error
		br            []byte
		ad            *Ad
		responseBytes []byte
	)
	if qres, serr = c.Get("campaigns", "ads", id, nil); serr != nil {
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
func DeleteAdById(id string) *DeError {
	glog.Info("ad to delete: " + id)
	if qres, err := c.Delete("campaigns", "ads", id, nil); err != nil {
		return NewError(500, err)
	} else {
		if !qres.Found {
			errstr := "ad id " + id + " does not exist"
			return NewError(500, errstr)
		}
	}
	return nil
}

func PostAd(req *http.Request) ([]byte, *DeError) {
	var (
		ad   Ad
		err  *DeError
		serr error
	)
	decoder := json.NewDecoder(req.Body)
	if serr = decoder.Decode(&ad); serr != nil {
		return nil, NewError(500, serr)
	} else if ad.Id == "" {
		return nil, NewError(400, "no ad_id found")
	} else if err = IndexAd(&ad); err != nil {
		return nil, NewError(500, err)
	} else {
		//success
		return nil, nil
	}
}

func GetAdIdsByCampaign(cid string) ([]string, *DeError) {
	var (
		serr    error
		sresult elastigo.SearchResult
		ids     []string
	)
	q := `{"filter": {"bool": {"must": [{"term": {"campaign":"` + cid + `"}}]}},"fields": []}`
	if sresult, serr = c.Search("campaigns", "ads", nil, q); serr != nil {
		return nil, NewError(500, serr)
	}
	for _, v := range sresult.Hits.Hits {
		ids = append(ids, v.Id)
	}
	return ids, nil
}
func GetESLastUpdated(col string) time.Time {
	var err error
	var result elastigo.SearchResult
	var sTime time.Time
	var responseBytes []byte
	type TType struct {
		Updated_Ad       []time.Time `json:"_updated_ad,omitempty"`
		Updated_Campaign []time.Time `json:"_updated_campaign,omitempty"`
	}
	var lastUpdated TType

	q := `{ "size":1, "fields":["` + col + `"], "query" : { "match_all":{} } , "sort" : [ { "` + col + `" : { "order":"desc" } } ] }`

	result, err = c.Search("campaigns", "ads", nil, q)
	if &result != nil && &result.Hits != nil && len(result.Hits.Hits) > 0 && result.Hits.Hits[0].Fields != nil {
		if responseBytes, err = result.Hits.Hits[0].Fields.MarshalJSON(); err != nil {
			glog.Info("Unable to Marshall from ES. Using default date: ", time.Time{})
			return sTime
		} else {
			if err = json.Unmarshal(responseBytes, &lastUpdated); err != nil {
				glog.Info("Unable to Unmarshall last updated from ES. Using default date - ", err)
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
