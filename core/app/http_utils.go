package app

import (
	"net/http"
)

func doRequest(client *http.Client, req *http.Request) (*http.Response, error) {
	return client.Do(req)
}
