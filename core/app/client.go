package app

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"time"
)

func newClient(proxy string, maxConnsPerHost int) *http.Client {
	logger.Debugf("initializing http request client with max %d connections per host", maxConnsPerHost)
	if proxy != "" {
		logger.Debugf("using proxy %s", proxy)
	}

	transport := &http.Transport{
		MaxIdleConns:          maxConnsPerHost * 2,
		MaxIdleConnsPerHost:   maxConnsPerHost,
		MaxConnsPerHost:       maxConnsPerHost,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   20 * time.Second,
		DisableKeepAlives:     false,
		DisableCompression:    true,
		ForceAttemptHTTP2:     false,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialer := &net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}
			return dialer.DialContext(ctx, network, addr)
		},
	}

	if proxy != "" {
		p, err := url.Parse(proxy)
		if err != nil {
			logger.Fatalf("failed to parse proxy string: %v", err)
		}
		transport.Proxy = http.ProxyURL(p)
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   10 * time.Minute,
	}

	return client
}
