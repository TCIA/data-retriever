package app

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// HTTPVersion1_1 forces the transport to stay on HTTP/1.1 even when a server
// advertises HTTP/2 support. HTTPVersion2 lets the transport negotiate
// HTTP/2 via ALPN where the server offers it (falling back to HTTP/1.1
// otherwise). Any other value (including "") behaves like HTTPVersion1_1,
// preserving this package's historical default. The two named values exist
// so a manifest can be run once under each for a side-by-side comparison.
const (
	HTTPVersion1_1 = "1.1"
	HTTPVersion2   = "2"
)

// ProtocolStats counts, per actually-negotiated HTTP protocol (as reported
// by net/http on each response, e.g. "HTTP/1.1" or "HTTP/2.0"), how many
// requests used it. Requesting HTTPVersion2 only asks the transport to
// attempt HTTP/2 via ALPN; a server that doesn't offer it still gets
// answered over HTTP/1.1, so this is the only reliable way to know which
// protocol a run actually used. Safe for concurrent use.
type ProtocolStats struct {
	mu     sync.Mutex
	counts map[string]int64
}

// NewProtocolStats returns an empty, ready-to-use ProtocolStats.
func NewProtocolStats() *ProtocolStats {
	return &ProtocolStats{counts: make(map[string]int64)}
}

func (s *ProtocolStats) record(proto string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.counts[proto]++
	s.mu.Unlock()
}

// Snapshot returns a copy of the current protocol -> request-count map.
func (s *ProtocolStats) Snapshot() map[string]int64 {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int64, len(s.counts))
	for k, v := range s.counts {
		out[k] = v
	}
	return out
}

// FormatProtocolStatsLine renders a snapshot as a single machine-parseable
// log line, e.g. "HTTP-PROTOCOL-STATS: HTTP/1.1=42 HTTP/2.0=0".
func FormatProtocolStatsLine(counts map[string]int64) string {
	protos := make([]string, 0, len(counts))
	for p := range counts {
		protos = append(protos, p)
	}
	sort.Strings(protos)

	parts := make([]string, 0, len(protos))
	for _, p := range protos {
		parts = append(parts, fmt.Sprintf("%s=%d", p, counts[p]))
	}
	if len(parts) == 0 {
		parts = append(parts, "none=0")
	}
	return "HTTP-PROTOCOL-STATS: " + strings.Join(parts, " ")
}

// protoRecordingTransport wraps a RoundTripper to record which protocol
// net/http actually negotiated for each response.
type protoRecordingTransport struct {
	http.RoundTripper
	stats *ProtocolStats
}

func (t *protoRecordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.RoundTripper.RoundTrip(req)
	if resp != nil {
		t.stats.record(resp.Proto)
	}
	return resp, err
}

func newClient(proxy string, maxConnsPerHost int, httpVersion string) *http.Client {
	return newClientWithStats(proxy, maxConnsPerHost, httpVersion, nil)
}

// newClientWithStats builds a client identical to newClient, but when stats
// is non-nil every request's actually-negotiated protocol is tallied into
// it (see ProtocolStats). Used by the CLI to report whether an HTTPVersion2
// run actually got HTTP/2 from the server or silently fell back to 1.1.
func newClientWithStats(proxy string, maxConnsPerHost int, httpVersion string, stats *ProtocolStats) *http.Client {
	logger.Debugf("initializing http request client with max %d connections per host (HTTP/%s)", maxConnsPerHost, httpVersion)
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
		ForceAttemptHTTP2:     httpVersion == HTTPVersion2,
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

	if httpVersion != HTTPVersion2 {
		// TLSNextProto set to a non-nil, empty map disables the transport's
		// HTTP/2 upgrade path entirely, forcing every TLS connection to stay
		// on HTTP/1.1 even if the server advertises h2 via ALPN.
		transport.TLSNextProto = make(map[string]func(authority string, c *tls.Conn) http.RoundTripper)
	}

	if proxy != "" {
		p, err := url.Parse(proxy)
		if err != nil {
			logger.Fatalf("failed to parse proxy string: %v", err)
		}
		transport.Proxy = http.ProxyURL(p)
	}

	// No client-wide Timeout: it would bound the entire request (connect
	// through full body read) and override the size-aware per-request
	// context deadlines set in download.go's downloadFromTCIA/downloadDirect.
	var rt http.RoundTripper = transport
	if stats != nil {
		rt = &protoRecordingTransport{RoundTripper: transport, stats: stats}
	}

	client := &http.Client{
		Transport: rt,
	}

	return client
}

// NewSharedHTTPClient builds a client suitable for reuse across multiple
// concurrently running manifests (see Options.SharedHTTPClient). It never
// carries a proxy since callers sharing a single client can't hand it a
// per-manifest proxy anyway.
func NewSharedHTTPClient(maxConnsPerHost int, httpVersion string) *http.Client {
	return newClient("", maxConnsPerHost, httpVersion)
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
