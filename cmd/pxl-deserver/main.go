package main

import (
	"encoding/json"
	"flag"
	de "github.com/dailymotion/pixelle-de/delib"
	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"net/http"
)

type AppHandler func(http.ResponseWriter, *http.Request) ([]byte, *de.DeError)

func (fn AppHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if response, e := fn(w, r); e != nil { // e is *de.DeError, not os.Error.
		setErrorResponse(w, e)
	} else {
		setResponse(w, http.StatusOK, response)
	}
}

func PostAdUnit(resp http.ResponseWriter, req *http.Request) ([]byte, *de.DeError) {
	return de.PostAdUnit(req)
}

func DeleteAdUnit(resp http.ResponseWriter, req *http.Request) ([]byte, *de.DeError) {
	var (
		id string
	)
	if id = mux.Vars(req)["id"]; id == "" {
		return nil, de.NewError(400, "no id provided")
	} else {
		return nil, de.DeleteAdUnitById(id)
	}
}
func GetAdUnit(resp http.ResponseWriter, req *http.Request) ([]byte, *de.DeError) {
	var (
		id string
	)
	if id = mux.Vars(req)["id"]; id == "" {
		return nil, de.NewError(400, "no id provided")
	} else {
		return de.GetAdUnitById(id)
	}
}

func PostESQuery(resp http.ResponseWriter, req *http.Request) ([]byte, *de.DeError) {
	return de.ProcessESQuery(req)
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

	rtr := mux.NewRouter()
	rtr.Handle("/healthcheck", AppHandler(PostESQuery)).Methods("GET")

	rtr.Handle("/ads/{id}", AppHandler(GetAdUnit)).Methods("GET")
	rtr.Handle("/ads", AppHandler(PostAdUnit)).Methods("PUT")
	rtr.Handle("/ads", AppHandler(PostAdUnit)).Methods("POST")
	rtr.Handle("/ads/{id}", AppHandler(DeleteAdUnit)).Methods("DELETE")

	rtr.Handle("/query", AppHandler(PostESQuery)).Methods("GET")
	rtr.Handle("/query", AppHandler(PostESQuery)).Methods("POST")
	//there is no such thing as deleting or updating a query

	http.Handle("/", rtr)

	if err := http.ListenAndServe(":7001", nil); err != nil {
		glog.Error("Error starting server, port 7001 is likely in use")
	} else {
		glog.Infoln("Listening on port 7001...")
	}
}
