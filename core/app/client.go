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
		ResponseHeaderTimeout: 300 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialer := &net.Dialer{
				Timeout:   300 * time.Second,
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

// NewSharedHTTPClient builds a client suitable for reuse across multiple
// concurrently running manifests (see Options.SharedHTTPClient). It never
// carries a proxy since callers sharing a single client can't hand it a
// per-manifest proxy anyway.
func NewSharedHTTPClient(maxConnsPerHost int) *http.Client {
	return newClient("", maxConnsPerHost)
}

// SetMaxConnsPerHost resizes a client's per-host connection cap in place,
// the same way WorkerSemaphore.SetLimit resizes the download concurrency
// limit. It takes effect for connections dialed after the call; connections
// already open or already admitted under the old limit are unaffected. A
// no-op if client wasn't built by this package (i.e. its Transport isn't an
// *http.Transport).
func SetMaxConnsPerHost(client *http.Client, maxConnsPerHost int) {
	if client == nil {
		return
	}
	if maxConnsPerHost < 1 {
		maxConnsPerHost = 1
	}
	t, ok := client.Transport.(*http.Transport)
	if !ok {
		return
	}
	t.MaxConnsPerHost = maxConnsPerHost
	t.MaxIdleConnsPerHost = maxConnsPerHost
	t.MaxIdleConns = maxConnsPerHost * 2
}
