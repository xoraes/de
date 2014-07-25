package main

import (
	"flag"
	"github.com/dailymotion/pixelle-de"
	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/mattbaird/elastigo/api"
	"net/http"
	"encoding/json"
)

type AppHandler func(http.ResponseWriter, *http.Request) ([]byte, *de.DeError)

func (fn AppHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if response, e := fn(w, r); e != nil { // e is *de.DeError, not os.Error.
		setErrorResponse(w, e)
	} else {
		setResponse(w, http.StatusOK, response)
	}
}

func PostAd(resp http.ResponseWriter, req *http.Request) ([]byte, *de.DeError) {
	return de.PostAdToES(req)
}

func DeleteAd(resp http.ResponseWriter, req *http.Request) ([]byte, *de.DeError) {
	var (
		id string
	)
	if id = mux.Vars(req)["id"]; id == "" {
		return nil, de.NewError(400, "no id provided")
	} else {
		return nil, de.DeleteAdById(id)
	}
}
func GetAd(resp http.ResponseWriter, req *http.Request) ([]byte, *de.DeError) {
	var (
		id string
	)
	if id = mux.Vars(req)["id"]; id == "" {
		return nil, de.NewError(400, "no id provided")
	} else {
		return de.GetAdById(id)
	}
}

func PostQuery(resp http.ResponseWriter, req *http.Request) ([]byte, *de.DeError) {
	return de.ProcessQuery(req)
}

//since ResponseWriter is an interface and has a pointer inside, we pass it by value
// and not by reference. see http://stackoverflow.com/questions/22157514/passing-http-responsewriter-by-value-or-reference
func setResponse(resp http.ResponseWriter, status int, body []byte) {
	resp.Header().Set("Content-Type", "application/json; charset=utf-8")
	resp.WriteHeader(status)
	resp.Write(body)
	glog.Info(`{"status":`, status, `,"headers":`, `"`, resp.Header(), `"`, `,"body":`, `"`, string(body), `"}`)
}
func setErrorResponse(resp http.ResponseWriter, err *de.DeError) {
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

	var (
		eshost string
		esport string
	)

	flag.StringVar(&eshost, "eshost", "localhost", "Elasticsearch host ip or hostname")
	flag.StringVar(&esport, "esport", "9200", "Elasticsearch port")
	flag.Parse()
	api.Domain = eshost
	api.Port = esport

	rtr := mux.NewRouter()
	rtr.Handle("/healthcheck", AppHandler(PostQuery)).Methods("GET")

	rtr.Handle("/de/ads/{id}", AppHandler(GetAd)).Methods("GET")
	rtr.Handle("/de/ads", AppHandler(PostAd)).Methods("PUT")
	rtr.Handle("/de/ads", AppHandler(PostAd)).Methods("POST")
	rtr.Handle("/de/ads/{id}", AppHandler(DeleteAd)).Methods("DELETE")

	rtr.Handle("/de/query", AppHandler(PostQuery)).Methods("GET")
	rtr.Handle("/de/query", AppHandler(PostQuery)).Methods("POST")
	rtr.Handle("/de/query", AppHandler(PostQuery)).Methods("PUT")
	//there is no such thing as deleting a query

	http.Handle("/", rtr)


	if err := http.ListenAndServe(":7001", nil); err != nil {
		glog.Error("Error starting server, port 7001 is likely in use")
	} else {
		glog.Infoln("Listening on port 7001...")
	}
}
