package app

import (
	"net/http"
)

func doRequest(client *http.Client, req *http.Request) (*http.Response, error) {
	resp, err := client.Do(req)
	if err == nil {
		logger.Debugf("HTTP response from %s used %s", req.URL.Host, resp.Proto)
	}
	return resp, err
}
