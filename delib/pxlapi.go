package delib

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

var (
	username = flag.String("user", "", "api super username")
	password = flag.String("pass", "", "api super password")
	env      = flag.String("env", "prod", "test or prod")
	host     string
	tr       = &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	client   = &http.Client{Transport: tr}
)

func GetUpdatedAdUnits(t time.Time) (*AdUnits, error) {
	var (
		urlStr  string
		resp    *http.Response
		err     error
		body    []byte
		adunits AdUnits
	)
	switch *env {
	case "test":
		host = "https://api.pxlad.in/adunits"
	default:
		host = "https://api.pxlad.io/adunits"
	}

	switch t.IsZero() {
	case true:
		u, _ := url.Parse(host + "?where=status==active")
		u.RawQuery = u.Query().Encode()
		urlStr = u.String()
	default:
		u, _ := url.Parse(host + "?where=_updated>" + "\"" + t.Format(time.RFC3339) + "\"")
		u.RawQuery = u.Query().Encode()
		urlStr = u.String()
	}
	req, _ := http.NewRequest("GET", urlStr, nil)
	req.SetBasicAuth(*username, *password)
	if resp, err = client.Do(req); err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, errors.New("API server error: " + strconv.Itoa(resp.StatusCode) + " Status:" + resp.Status)
	}
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if err = json.Unmarshal(body, &adunits); err != nil {
		return nil, err
	}
	return &adunits, nil
}
