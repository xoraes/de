package main

import (
	"encoding/json"
	"flag"
	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/mattbaird/elastigo/api"
	"net/http"
)

type appHandler func(http.ResponseWriter, *http.Request) ([]byte, *DeError)

func (fn appHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if response, e := fn(w, r); e != nil { // e is *DeError, not os.Error.
		setErrorResponse(w, e)
	} else {
		setResponse(w, http.StatusOK, response)
	}
}

func postAd(resp http.ResponseWriter, req *http.Request) ([]byte, *DeError) {
	return postAdToES(req)
}

func deleteAd(resp http.ResponseWriter, req *http.Request) ([]byte, *DeError) {
	var (
		id string
	)
	if id = mux.Vars(req)["id"]; id == "" {
		return nil, NewError(400, "no id provided")
	} else {
		return nil, deleteAdById(id)
	}
}
func getAd(resp http.ResponseWriter, req *http.Request) ([]byte, *DeError) {
	var (
		id string
	)
	if id = mux.Vars(req)["id"]; id == "" {
		return nil, NewError(400, "no id provided")
	} else {
		return getAdById(id)
	}
}

func postQuery(resp http.ResponseWriter, req *http.Request) ([]byte, *DeError) {
	return processQuery(req)
}

//since ResponseWriter is an interface and has a pointer inside, we pass it by value
// and not by reference. see http://stackoverflow.com/questions/22157514/passing-http-responsewriter-by-value-or-reference
func setResponse(resp http.ResponseWriter, status int, body []byte) {
	resp.Header().Set("Content-Type", "application/json; charset=utf-8")
	resp.WriteHeader(status)
	resp.Write(body)
	glog.Info(`{"status":`, status, `,"headers":`, `"`, resp.Header(), `"`, `,"body":`, `"`, string(body), `"}`)
}
func setErrorResponse(resp http.ResponseWriter, err *DeError) {
	var responseBytes []byte
	var serr error
	if responseBytes, serr = json.MarshalIndent(err, "", "    "); serr != nil {
		panic(serr)
	}
	resp.Header().Set("Content-Type", "application/json; charset=utf-8")
	resp.WriteHeader(err.ErrorCode())
	resp.Write(responseBytes)
	glog.Error(`{"status":`, err.ErrorCode(), `,"headers":`, `"`, resp.Header(), `"`, `,"body":`, `"`, string(responseBytes), `"}`)
}

func main() {

	var eshost string

	flag.StringVar(&eshost, "eshost", "localhost", "elasticsearch host ip or hostname")
	flag.Parse()
	api.Domain = eshost
	api.Port = "9200"

	rtr := mux.NewRouter()
	rtr.Handle("/healthcheck", appHandler(postQuery)).Methods("GET")

	rtr.Handle("/de/ads/{id}", appHandler(getAd)).Methods("GET")
	rtr.Handle("/de/ads", appHandler(postAd)).Methods("PUT")
	rtr.Handle("/de/ads", appHandler(postAd)).Methods("POST")
	rtr.Handle("/de/ads/{id}", appHandler(deleteAd)).Methods("DELETE")

	rtr.Handle("/de/query", appHandler(postQuery)).Methods("GET")
	rtr.Handle("/de/query", appHandler(postQuery)).Methods("POST")
	rtr.Handle("/de/query", appHandler(postQuery)).Methods("PUT")
	//there is no such thing as deleting a query

	http.Handle("/", rtr)

	glog.Infoln("Listening on port 3000...")
	http.ListenAndServe(":3000", nil)
}
