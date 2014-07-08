package main

import (
	"encoding/json"
	"errors"
	"flag"
	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/mattbaird/elastigo/api"
	"github.com/mattbaird/elastigo/core"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// add single go struct entity
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
	TacticId          string   `json:"tactic_id,omitempty"`
	ChannelId         string   `json:"channel_id,omitempty"`
	ChannelUrl        string   `json:"channel_url,omitempty"`
	IsPaused          bool     `json:"is_paused,omitempty"`
	GoalReached       bool     `json:"goal_reached,omitempty"`
}
type SearchQuery struct {
	Languages []string `json:"languages,omitempty"`
	Locations []string `json:"locations,omitempty"`
	AdFormat  int      `json:"ad_format,omitempty"`
}

func (ad Ad) marshallAd() ([]byte, error) {
	jsonBytes, err := json.Marshal(ad)
	return jsonBytes, err
}
func (ad Ad) createAd() (response api.BaseResponse, err error) {
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

	jsonBytes, err := ad.marshallAd()
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
func postAd(resp http.ResponseWriter, req *http.Request) {
	var ad Ad
	decoder := json.NewDecoder(req.Body)

	var err error

	if err = decoder.Decode(&ad); err != nil || ad.AdId == "" {
		setErrorResponse(resp, http.StatusInternalServerError, err)
	} else if _, err = ad.createAd(); err != nil {
		setErrorResponse(resp, http.StatusInternalServerError, err)
	} else {
		setResponse(resp, http.StatusCreated, nil)
	}
}

func deleteAd(resp http.ResponseWriter, req *http.Request) {
	var key string
	if key = mux.Vars(req)["id"]; key == "" {
		setErrorResponse(resp, http.StatusBadRequest, errors.New("no id provided"))
	} else {
		glog.Info("key to delete: " + key)
		if qres, err := core.Delete("campaigns", "ads", key, nil); err != nil {
			setErrorResponse(resp, http.StatusInternalServerError, err)
		} else {
			if qres.Found {
				setResponse(resp, http.StatusOK, nil)
			} else {
				errstr := "ad id " + key + " does not exist"
				setErrorResponse(resp, http.StatusBadRequest, errors.New(errstr))
			}
		}
	}
}
func getAd(resp http.ResponseWriter, req *http.Request) {
	var key string
	var responseBytes []byte
	if key = mux.Vars(req)["id"]; key == "" {
		setErrorResponse(resp, http.StatusBadRequest, errors.New("no id provided"))
	} else {
		glog.Info(" querying for ad id: " + key)
		if qres, err := core.Get("campaigns", "ads", key, nil); err != nil {
			setErrorResponse(resp, http.StatusInternalServerError, err)
		} else if qres.Found {
			if responseBytes, err = json.Marshal(qres.Source); err != nil {
				setErrorResponse(resp, http.StatusInternalServerError, err)
			} else {
				setResponse(resp, http.StatusOK, responseBytes)
			}
		} else {
			errstr := "ad id " + key + " does not exist"
			setErrorResponse(resp, http.StatusBadRequest, errors.New(errstr))
		}
	}
}

func postQuery(resp http.ResponseWriter, req *http.Request) {
	var sq SearchQuery
	var responseBytes []byte
	var err error
	var qresult core.SearchResult

	decoder := json.NewDecoder(req.Body)
	err = decoder.Decode(&sq)

	if err != nil && err != io.EOF { //if err is EOF, it means the query has no body, so we create a generic query and return result
		setErrorResponse(resp, http.StatusBadRequest, err)
	} else if qresult, err = sq.queryES(); err != nil {
		setErrorResponse(resp, http.StatusInternalServerError, err)
	} else if &qresult != nil && &qresult.Hits != nil && len(qresult.Hits.Hits) > 0 {
		if responseBytes, err = json.Marshal(qresult.Hits.Hits[0].Source); err != nil {
			setErrorResponse(resp, http.StatusInternalServerError, err)
		} else {
			setResponse(resp, http.StatusOK, responseBytes)
		}
	} else {
		setErrorResponse(resp, http.StatusNoContent, errors.New("no match was found for the given query"))
	}
}

func formatError(e string) string {
	return `{"error":"` + e + `"}`
}

//since ResponseWriter is an interface and has a pointer inside, we pass it by value
// and not by reference. see http://stackoverflow.com/questions/22157514/passing-http-responsewriter-by-value-or-reference
func setResponse(resp http.ResponseWriter, status int, body []byte) {
	resp.Header().Set("Content-Type", "application/json; charset=utf-8")
	resp.WriteHeader(status)
	resp.Write(body)
	glog.Info(`{"status":`, status, `,"headers":`, `"`, resp.Header(), `"`, `,"body":`, `"`, string(body), `"}`)
}
func setErrorResponse(resp http.ResponseWriter, status int, err error) {
	e := err.Error()
	body := formatError(e)
	//setResponse(resp,status,[]byte(body))
	resp.Header().Set("Content-Type", "application/json; charset=utf-8")
	resp.WriteHeader(status)
	resp.Write([]byte(body))
	glog.Error(`{"status":`, status, `,"headers":`, `"`, resp.Header(), `"`, `,"body":`, `"`, body, `"}`)
}

func main() {

	var eshost string

	flag.StringVar(&eshost, "eshost", "es.pxlad.in", "elasticsearch host ip or hostname")
	flag.Parse()
	api.Domain = eshost
	api.Port = "9200"

	rtr := mux.NewRouter()
	rtr.HandleFunc("/healthcheck", postQuery).Methods("GET")

	rtr.HandleFunc("/de/ads/{id}", getAd).Methods("GET")
	rtr.HandleFunc("/de/ads", postAd).Methods("PUT")
	rtr.HandleFunc("/de/ads", postAd).Methods("POST")
	rtr.HandleFunc("/de/ads/{id}", deleteAd).Methods("DELETE")

	rtr.HandleFunc("/de/query", postQuery).Methods("GET")
	rtr.HandleFunc("/de/query", postQuery).Methods("POST")
	rtr.HandleFunc("/de/query", postQuery).Methods("PUT")
	//there is no such thing as deleting a query

	http.Handle("/", rtr)

	glog.Infoln("Listening on port 3000...")
	http.ListenAndServe(":3000", nil)
}
