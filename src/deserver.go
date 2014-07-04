package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/mattbaird/elastigo/api"
	"github.com/mattbaird/elastigo/core"
	"log"
	"net/http"
	"strings"
	"io"
)

// add single go struct entity
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
type SearchQuery struct {
	Languages []string `json:"languages,omitempty"`
	Locations []string `json:"locations,omitempty"`
	AdFormat  string   `json:"ad_format,omitempty"`
}

func (ad Ad) marshallAd() ([]byte, error) {
	jsonBytes, err := json.Marshal(ad)
	return jsonBytes, err
}
func (ad Ad) CreateAd() (response api.BaseResponse, err error) {
	var index int
	for index, _ = range ad.Languages {
		ad.Languages[index] = strings.ToLower(ad.Languages[index])
	}
	for index, _ = range ad.Locations {
		ad.Locations[index] = strings.ToLower(ad.Locations[index])
	}
	//doing this for consistency, ad format is generally just a stringified number but that might change?
	for index, _ = range ad.AdFormats {
		ad.AdFormats[index] = strings.ToLower(ad.AdFormats[index])
	}
	for index, _ = range ad.ExcludedLocations {
		ad.ExcludedLocations[index] = strings.ToLower(ad.ExcludedLocations[index])
	}

	jsonBytes, err := ad.marshallAd()
	if response, err = core.Index("campaigns", "ads", ad.AdId, nil, jsonBytes); err != nil {
		//TODO :retry logic goes here
		fmt.Println(err)
	}
	fmt.Println(response)
	return response, err
}
func (sq SearchQuery) QueryES() (core.SearchResult, error) {
	sresult, err := core.SearchRequest("campaigns", "ads", nil, sq.CreateESQueryString())
	return sresult, err
}
func (sq SearchQuery) CreateESQueryString() (q string) {
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
	useBoolFilter := len(sq.Locations) > 0 || len(sq.Languages) > 0 || len(sq.AdFormat) > 0

	if useBoolFilter {
		q += `"bool":{"must":[`
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

		if len(sq.AdFormat) > 0 {
			var af string
			af = string('"') + sq.AdFormat + string('"')
			q += delim + `{ "query":  {"term": { "ad_formats":` + af + `}}}`
			delim = ","
		}
		q += `]`

		if len(sq.Locations) > 0 && locerr == nil {
			q += delim + `"must_not":[`
			//is_paused and goal_reached are separate fields
			//so they can be changed independently
			q += `{ "query":  {"terms": { "is_paused": true}}},`
			q += `{ "query":  {"terms": { "goal_reached": true}}},`
			q += `{ "query":  {"terms": { "excluded_locations":` + string(loc) + `}}}]`
			delim = ","
		}
		q += "}"
	}
	q += `}}},"random_score": {}}}}`

	log.Println("==== Generated ES query ====>")
	log.Println(q)
	log.Println("=============================")
	return q
}
func postAd(resp http.ResponseWriter,req *http.Request) {
	var ad Ad
	decoder := json.NewDecoder(req.Body)

	var err error

	if err = decoder.Decode(&ad); err != nil || ad.AdId == "" {
		resp.WriteHeader(http.StatusInternalServerError)
	} else if _, err = ad.CreateAd(); err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
		log.Println(err)
	} else {
		resp.WriteHeader(http.StatusCreated)
	}
}

func deleteAd(resp http.ResponseWriter,req *http.Request) {
	var key string
	if key = mux.Vars(req)["id"]; key == "" {
		resp.WriteHeader(http.StatusBadRequest)
		resp.Write(ErrorMessage("no id provided"))
	} else {
		log.Println("key to delete: " + key)
		if qres, err := core.Delete("campaigns", "ads", key, nil); err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
		} else {
			if qres.Found {
				resp.WriteHeader(http.StatusOK)
			} else {
				//response should have this order: writeheader and then write bytes
				resp.WriteHeader(http.StatusBadRequest)
				errstr := "ad id " + key + " does not exist"
				resp.Write(ErrorMessage(errstr))
				log.Println(errstr)
			}
		}
	}
}
func getAd(resp http.ResponseWriter,req *http.Request) {
	var key string
	var responseBytes []byte
	if key = mux.Vars(req)["id"]; key == "" {
		resp.WriteHeader(http.StatusBadRequest)
		resp.Write(ErrorMessage("no id provided"))
	} else {
		log.Println(" querying for ad id: " + key)
		if qres, err := core.Get("campaigns", "ads", key, nil); err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println(err)
		} else if qres.Found {
				if responseBytes, err = json.Marshal(qres.Source); err != nil {
					resp.WriteHeader(http.StatusInternalServerError)
					log.Println("Error while getting response from ES: ", err)
				} else {
					resp.WriteHeader(http.StatusOK)
					resp.Write(responseBytes)
				}
		} else {
				//response should have this order: writeheader and then write bytes
				resp.WriteHeader(http.StatusBadRequest)
				errstr := "ad id " + key + " does not exist"
				resp.Write(ErrorMessage(errstr))
				log.Println(errstr)
		}
	}
}


func postQuery(resp http.ResponseWriter,req *http.Request) {
	var sq SearchQuery
	var responseBytes []byte
	var err error
	var qresult core.SearchResult

	decoder := json.NewDecoder(req.Body)
	err = decoder.Decode(&sq)
	if err != nil && err != io.EOF {
		log.Println("Error while decoding request:", err)
		resp.WriteHeader(http.StatusBadRequest)
	} else if qresult, err = sq.QueryES(); err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
		log.Println("Error while querying ES: ", err)
	} else if &qresult != nil && &qresult.Hits != nil && len(qresult.Hits.Hits) > 0 {
		if responseBytes, err = json.Marshal(qresult.Hits.Hits[0].Source); err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			log.Println("Error while getting response from ES: ", err)
		} else {
			resp.WriteHeader(http.StatusOK)
			resp.Write(responseBytes)
		}
	} else {
		resp.WriteHeader(http.StatusNoContent)
		log.Println("no match was found for the given query")
	}
}

func ErrorMessage(message string) []byte {
	return []byte(`{"error": "` + message + `"}`)
}
func main() {

	var eshost string
	//var ad Ad
	//ad.SearchAd(nil.nil.nil)
	flag.StringVar(&eshost, "eshost", "es.pxlad.in", "elasticsearch host ip or hostname")
	flag.Parse()
	api.Domain = eshost
	api.Port = "9200"
	var q SearchQuery
	//q.AdFormats = "9"
	q.Languages = []string{"fr"}
	//q.Locations = []string{"fr", "fr:1234"}

	qresult, _ := q.QueryES()
	var responseBody []byte
	if &qresult != nil && &qresult.Hits != nil && len(qresult.Hits.Hits) > 0 {
		responseBody, _ = json.Marshal(qresult.Hits.Hits[0].Source)
	} else {
		responseBody = ErrorMessage("no matches found")
	}

	log.Println(string(responseBody))

	rtr := mux.NewRouter()
	rtr.HandleFunc("/de/ads/{id}", getAd).Methods("GET")
	rtr.HandleFunc("/de/ads", postAd).Methods("PUT")
	rtr.HandleFunc("/de/ads", postAd).Methods("POST")
	rtr.HandleFunc("/de/ads/{id}", deleteAd).Methods("DELETE")


	rtr.HandleFunc("/de/query", postQuery).Methods("GET")
	rtr.HandleFunc("/de/query", postQuery).Methods("POST")
	rtr.HandleFunc("/de/query", postQuery).Methods("PUT")
	//there is no such thing as deleting a query

	http.Handle("/", rtr)

	log.Println("Listening...")
	http.ListenAndServe(":3000", nil)
}
