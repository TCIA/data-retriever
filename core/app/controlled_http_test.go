package app

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type controlledRoundTripFunc func(*http.Request) (*http.Response, error)

func (f controlledRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// Route requests through a local TLS server without weakening production host
// validation or using real credentials/network endpoints.
func controlledTestClient(t *testing.T, handler http.HandlerFunc) *http.Client {
	t.Helper()
	srv := httptest.NewTLSServer(handler)
	t.Cleanup(srv.Close)
	destination, _ := url.Parse(srv.URL)
	transport := srv.Client().Transport
	return &http.Client{Transport: controlledRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		copy := req.Clone(req.Context())
		copy.Host = req.URL.Host
		copy.URL.Scheme, copy.URL.Host = destination.Scheme, destination.Host
		return transport.RoundTrip(copy)
	})}
}

func TestControlledDRSRejectsUntrustedAuthorityBeforeNetwork(t *testing.T) {
	client := &http.Client{Transport: controlledRoundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("invalid DRS identifier reached the network")
		return nil, errors.New("unexpected request")
	})}
	auth := &Gen3AuthManager{client: client, apiKey: "test-key"}
	for _, raw := range []string{
		"drs://evil.example/object", "https://nci-crdc.datacommons.io/object",
		"drs://nci-crdc.datacommons.io.evil.example/object", "drs://user@nci-crdc.datacommons.io/object",
		"drs://nci-crdc.datacommons.io:443/object", "drs://nci-crdc.datacommons.io/object?secret=key",
		"drs://nci-crdc.datacommons.io/object#fragment", "drs://nci-crdc.datacommons.io/../object",
		"drs://nci-crdc.datacommons.io/%2e%2e/object", "drs://nci-crdc.datacommons.io/",
	} {
		t.Run(raw, func(t *testing.T) {
			info := &FileInfo{DRSURI: raw}
			if err := info.downloadFromGen3(context.Background(), t.TempDir(), client, auth, &Options{}, nil); err == nil {
				t.Fatal("accepted invalid URI")
			}
		})
	}
}

func TestControlledDownloadRefreshesOnceAndKeepsSecretsOutOfLogs(t *testing.T) {
	var tokenCalls, authorizationCalls, storageCalls atomic.Int32
	var log bytes.Buffer
	setLogger(true, true, "", &log)
	t.Cleanup(func() { setLogger(false, false, "") })
	client := controlledTestClient(t, func(w http.ResponseWriter, req *http.Request) {
		switch {
		case strings.HasSuffix(req.URL.Path, "/access_token"):
			if req.Host != controlledDRSHost {
				t.Errorf("credential sent to %q", req.Host)
			}
			body, _ := io.ReadAll(req.Body)
			if !strings.Contains(string(body), "secret-key") {
				t.Error("missing key")
			}
			fmt.Fprintf(w, `{"access_token":"secret-token-%d"}`, tokenCalls.Add(1))
		case strings.HasPrefix(req.URL.Path, "/user/data/download/"):
			if authorizationCalls.Add(1) == 1 {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if req.Header.Get("Authorization") != "Bearer secret-token-2" {
				t.Error("refreshed token not used")
			}
			fmt.Fprint(w, `{"url":"https://storage.example/file?signature=secret-signature"}`)
		case req.Host == "storage.example":
			storageCalls.Add(1)
			if req.Header.Get("Authorization") != "" {
				t.Error("bearer token reached storage")
			}
			if req.Header.Get("Accept-Encoding") != "identity" {
				t.Error("payload encoding not fixed")
			}
			fmt.Fprint(w, "payload")
		default:
			t.Errorf("unexpected request %s", req.URL.Path)
		}
	})
	auth := &Gen3AuthManager{client: client, apiKey: "secret-key"}
	info := &FileInfo{DRSURI: "drs://" + controlledDRSHost + "/dg.4DFC/object", FileName: "file.bin", FileSize: "7"}
	output := t.TempDir()
	if err := info.downloadFromGen3(context.Background(), output, client, auth, &Options{}, nil); err != nil {
		t.Fatal(err)
	}
	if tokenCalls.Load() != 2 || authorizationCalls.Load() != 2 || storageCalls.Load() != 1 {
		t.Fatal("unexpected request counts")
	}
	if info.DownloadURL != "" || info.sourceURL() != info.DRSURI {
		t.Fatal("temporary URL persisted on source metadata")
	}
	for _, secret := range []string{"secret-key", "secret-token", "secret-signature", "Authorization"} {
		if strings.Contains(log.String(), secret) {
			t.Errorf("log contains %q", secret)
		}
	}
}

func TestControlled401RefreshIsBounded(t *testing.T) {
	var tokens, requests atomic.Int32
	client := controlledTestClient(t, func(w http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "/access_token") {
			tokens.Add(1)
			fmt.Fprint(w, `{"access_token":"token"}`)
			return
		}
		requests.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	})
	auth := &Gen3AuthManager{client: client, apiKey: "test"}
	_, err := getGen3DownloadURLContext(context.Background(), client, controlledDRSHost, "object", auth)
	var status *httpStatusError
	if !errors.As(err, &status) || status.statusCode != http.StatusUnauthorized {
		t.Fatalf("unexpected error: %v", err)
	}
	if tokens.Load() != 2 || requests.Load() != 2 {
		t.Fatalf("unbounded retry: %d tokens, %d requests", tokens.Load(), requests.Load())
	}
}

func TestControlledAnonymousDRSAndServiceRedirects(t *testing.T) {
	for _, credential := range []bool{false, true} {
		for _, redirect := range []bool{false, true} {
			t.Run(fmt.Sprintf("credentials=%t/redirect=%t", credential, redirect), func(t *testing.T) {
				var requests atomic.Int32
				client := controlledTestClient(t, func(w http.ResponseWriter, req *http.Request) {
					requests.Add(1)
					if req.Host != controlledDRSHost {
						t.Fatal("service redirect followed")
					}
					if redirect {
						http.Redirect(w, req, "https://sub.nci-crdc.datacommons.io/steal", http.StatusTemporaryRedirect)
						return
					}
					if strings.HasSuffix(req.URL.Path, "/access_token") {
						fmt.Fprint(w, `{"access_token":"test-token"}`)
						return
					}
					if !credential && req.Header.Get("Authorization") != "" {
						t.Error("anonymous request has authorization")
					}
					fmt.Fprint(w, `{"url":"https://storage.example/file"}`)
				})
				var auth *Gen3AuthManager
				if credential {
					auth = &Gen3AuthManager{client: client, apiKey: "key"}
				}
				location, err := getGen3DownloadURLContext(context.Background(), client, controlledDRSHost, "object", auth)
				if redirect {
					if err == nil || requests.Load() != 1 {
						t.Fatalf("redirect not refused: %v (%d requests)", err, requests.Load())
					}
				} else if err != nil || location != "https://storage.example/file" {
					t.Fatalf("anonymous/supported route failed: %v", err)
				}
			})
		}
	}
}

func TestControlledStorageURLValidationAndCancellation(t *testing.T) {
	for _, raw := range []string{"http://storage.example/file", "https://user:pass@storage.example/file", "javascript:secret", "https://storage.example/file#fragment", ""} {
		if validControlledStorageURL(raw) {
			t.Errorf("accepted %q", raw)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := controlledTestClient(t, func(http.ResponseWriter, *http.Request) { t.Error("cancelled request reached handler") })
	_, err := getGen3DownloadURLContext(ctx, client, controlledDRSHost, "object", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("lost cancellation: %v", err)
	}
}

func TestTokenExpiryIsCacheHint(t *testing.T) {
	for _, seconds := range []int64{-10, 3600} {
		claims := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d}`, time.Now().Unix()+seconds)))
		if got := tokenNearExpiry("header." + claims + ".signature"); got != (seconds < 0) {
			t.Errorf("expiry result %t for %d", got, seconds)
		}
	}
	if tokenNearExpiry("opaque") {
		t.Fatal("opaque tokens should rely on 401")
	}
}

func TestDirectIntegrityBeforePromotion(t *testing.T) {
	payload := []byte("payload")
	md5sum, sha256sum := md5.Sum(payload), sha256.Sum256(payload)
	for _, tc := range []struct {
		name, size, md5, sha256 string
		success                 bool
	}{
		{"verified", "7", hex.EncodeToString(md5sum[:]), hex.EncodeToString(sha256sum[:]), true},
		{"wrong size", "8", "", "", false},
		{"wrong checksum", "7", strings.Repeat("0", 32), "", false},
		{"malformed checksum", "7", "invalid", "", false},
		{"invalid size", "7 MB", "", "", false},
		{"no metadata", "", "", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write(payload) }))
			defer srv.Close()
			info := &FileInfo{DownloadURL: srv.URL, FileName: "file.bin", FileSize: tc.size, MD5: tc.md5, SHA256: tc.sha256}
			output, options := t.TempDir(), &Options{}
			final := filepath.Join(info.DcimFiles(output, options), info.FileName)
			if err := os.WriteFile(final, []byte("previous"), 0600); err != nil {
				t.Fatal(err)
			}
			err := info.downloadDirect(context.Background(), output, srv.Client(), options, nil, nil)
			if (err == nil) != tc.success {
				t.Fatalf("success=%t, err=%v", tc.success, err)
			}
			actual, _ := os.ReadFile(final)
			want := "previous"
			if tc.success {
				want = "payload"
			}
			if string(actual) != want {
				t.Fatalf("final = %q", actual)
			}
			if _, err := os.Stat(final + ".tmp"); !os.IsNotExist(err) {
				t.Fatalf("partial file retained: %v", err)
			}
		})
	}
}

func TestDirectTransportErrorRedactsSignedURL(t *testing.T) {
	client := &http.Client{Transport: controlledRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("timeout at %s", req.URL.String())
	})}
	info := &FileInfo{DownloadURL: "https://storage.example/file?signature=secret", FileName: "file.bin"}
	err := info.downloadDirect(context.Background(), t.TempDir(), client, &Options{}, nil, nil)
	if err == nil || strings.Contains(err.Error(), "secret") || !isRetryableError(err) {
		t.Fatalf("unsafe/unretryable error: %v", err)
	}
}

func TestSharedHTTPClientVerifiesTLSCertificate(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("untrusted TLS connection accepted") }))
	defer srv.Close()
	client := newClient("", 1, HTTPVersion1_1)
	_, err := client.Get(srv.URL)
	var unknown x509.UnknownAuthorityError
	if !errors.As(err, &unknown) {
		t.Fatalf("expected untrusted certificate error, got %v", err)
	}
}

func TestDirectManifestIntegrityAndExistingFile(t *testing.T) {
	root := t.TempDir()
	manifest := filepath.Join(root, "manifest.csv")
	text := "drs_uri,File Name,CTDC Size,MD5 Checksum,SHA256 Checksum\nobject,file.bin,7,321c3cf486ed509164edec1e1981fec8,239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5\n"
	if err := os.WriteFile(manifest, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	files, err := decodeSpreadsheet(manifest)
	if err != nil || len(files) != 1 {
		t.Fatalf("decode: %v", err)
	}
	info, options := files[0], &Options{}
	if info.FileSize != "7" || info.MD5 == "" || info.SHA256 == "" {
		t.Fatalf("integrity fields not parsed: %+v", info)
	}
	path := filepath.Join(info.DcimFiles(root, options), info.FileName)
	if err := os.WriteFile(path, []byte("payload"), 0600); err != nil {
		t.Fatal(err)
	}
	if info.NeedsDownload(root, false, false, options) {
		t.Fatal("verified existing direct file should be skipped")
	}
	if err := os.WriteFile(path, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if !info.NeedsDownload(root, false, false, options) {
		t.Fatal("equal-size corrupt direct file was skipped")
	}
}

func TestControlledStorageRedirectRejectsHTTP(t *testing.T) {
	var storage atomic.Int32
	client := controlledTestClient(t, func(w http.ResponseWriter, req *http.Request) {
		if req.Host == controlledDRSHost {
			fmt.Fprint(w, `{"url":"https://storage.example/file?signature=secret"}`)
			return
		}
		if storage.Add(1) > 1 {
			t.Error("unsafe storage redirect followed")
		}
		http.Redirect(w, req, "http://storage.example/insecure?signature=secret", http.StatusTemporaryRedirect)
	})
	info := &FileInfo{DRSURI: "drs://" + controlledDRSHost + "/object", FileName: "file.bin"}
	err := info.downloadFromGen3(context.Background(), t.TempDir(), client, nil, &Options{}, nil)
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("unsafe error: %v", err)
	}
	if storage.Load() != 1 {
		t.Fatalf("unsafe storage request count %d", storage.Load())
	}
}
