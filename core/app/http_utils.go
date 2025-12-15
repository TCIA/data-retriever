package app

import (
	"net/http"
	"strings"
)

// doRequest performs an HTTP request with automatic v2 -> v1 fallback.
func doRequest(client *http.Client, req *http.Request) (*http.Response, error) {
	originalURL := req.URL.String()

	resp, err := client.Do(req)
	if err != nil || !strings.Contains(originalURL, "/v2/") {
		return resp, err
	}

	if resp.StatusCode == 404 || (resp.StatusCode >= 500 && resp.StatusCode <= 504) {
		resp.Body.Close()

		v1URL := strings.Replace(originalURL, "/v2/", "/v1/", 1)
		v1Req, err := http.NewRequest(req.Method, v1URL, req.Body)
		if err != nil {
			return nil, err
		}

		v1Req.Header = req.Header.Clone()
		if req.Context() != nil {
			v1Req = v1Req.WithContext(req.Context())
		}

		logger.Warnf("v2 endpoint returned %d, falling back to v1: %s", resp.StatusCode, originalURL)
		logger.Infof("Attempting v1 endpoint: %s", v1URL)
		return client.Do(v1Req)
	}

	return resp, nil
}
