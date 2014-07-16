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
)
func postAd(resp http.ResponseWriter, req *http.Request) {
	var ad Ad
	decoder := json.NewDecoder(req.Body)

	var err error

	if err = decoder.Decode(&ad); err != nil || ad.AdId == "" {
		setErrorResponse(resp, http.StatusInternalServerError, err)
	} else if _, err = ad.indexAd(); err != nil {
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

	flag.StringVar(&eshost, "eshost", "localhost", "elasticsearch host ip or hostname")
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
